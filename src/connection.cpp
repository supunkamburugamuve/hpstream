#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <cinttypes>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include "connection.h"

#define HPS_POST(post_fn, comp_fn, seq, op_str, ...)        \
  do {                  \
    int timeout_save;            \
    int ret, rc;              \
                    \
    while (1) {              \
      ret = post_fn(__VA_ARGS__);        \
      if (!ret)            \
        break;            \
                    \
      if (ret != -FI_EAGAIN) {        \
        HPS_ERR("%s %d", op_str, ret);      \
        return ret;          \
      }              \
                    \
      timeout_save = timeout;          \
      timeout = 0;            \
      rc = comp_fn(seq);          \
      if (rc && rc != -FI_EAGAIN) {        \
        HPS_ERR("Failed to get %s completion\n", op_str);  \
        return rc;          \
      }              \
      timeout = timeout_save;          \
    }                \
    seq++;                \
  } while (0)


#define HPS_EP_BIND(ep, fd, flags)					\
	do {								\
		int ret;						\
		if ((fd)) {						\
			ret = fi_ep_bind((ep), &(fd)->fid, (flags));	\
			if (ret) {					\
				HPS_ERR("fi_ep_bind %d", ret);		\
				return ret;				\
			}						\
		}							\
	} while (0)


Connection::Connection(Options *opts, struct fi_info *info_hints, struct fi_info *info,
                       struct fid_fabric *fabric, struct fid_domain *domain, struct fid_eq *eq) {

  print_short_info(info);

  this->options = opts;
  this->info = info;
  this->info_hints = info_hints;
  this->fabric = fabric;
  this->domain = domain;

  this->txcq = NULL;
  this->rxcq = NULL;
  this->txcntr = NULL;
  this->rxcntr = NULL;

  this->ep = NULL;
  this->alias_ep = NULL;
  this->av = NULL;
  this->mr = NULL;
  this->no_mr = {};

  this->rx_fd = -1;
  this->tx_fd = -1;

  this->tx_buf = NULL;
  this->buf = NULL;
  this->rx_buf = NULL;

  this->buf_size = 0;
  this->tx_size = 0;
  this->rx_size = 0;
  this->recv_buf = NULL;
  this->send_buf = NULL;

  this->remote_cq_data = 0;
  this->waitset = NULL;

  this->cq_attr = {};
  this->cntr_attr = {};
  this->av_attr = {};
  this->tx_ctx = {};
  this->rx_ctx = {};

  this->tx_seq = 0;
  this->rx_seq = 0;
  this->tx_cq_cntr = 0;
  this->rx_cq_cntr = 0;

  this->ft_skip_mr = 0;

  this->cq_attr.wait_obj = FI_WAIT_NONE;
  this->cntr_attr.events = FI_CNTR_EVENTS_COMP;
  this->cntr_attr.wait_obj = FI_WAIT_NONE;

  this->av_attr.type = FI_AV_MAP;
  this->av_attr.count = 1;

  this->remote_fi_addr = FI_ADDR_UNSPEC;
  this->remote = {};

  this->timeout = -1;
}

void Connection::Free() {
  if (mr != &no_mr) {
    HPS_CLOSE_FID(mr);
  }
  HPS_CLOSE_FID(alias_ep);
  HPS_CLOSE_FID(ep);
  HPS_CLOSE_FID(rxcq);
  HPS_CLOSE_FID(txcq);
  HPS_CLOSE_FID(rxcntr);
  HPS_CLOSE_FID(txcntr);
  HPS_CLOSE_FID(av);
  HPS_CLOSE_FID(domain);
  HPS_CLOSE_FID(waitset);
  HPS_CLOSE_FID(fabric);
}

Buffer * Connection::ReceiveBuffer() {
  return this->recv_buf;
}

Buffer * Connection::SendBuffer() {
  return this->send_buf;
}

int Connection::AllocateActiveResources() {
  int ret;
  HPS_INFO("Allocating resources for the connection");
  if (info_hints->caps & FI_RMA) {
    ret = hps_utils_set_rma_caps(info);
    if (ret)
      return ret;
  }

  ret = AllocateBuffers();
  if (ret) {
    return ret;
  }

  // we use the context, not the counter
  cq_attr.format = FI_CQ_FORMAT_CONTEXT;

  // create a file descriptor wait cq set
  cq_attr.wait_obj = FI_WAIT_FD;
  cq_attr.wait_cond = FI_CQ_COND_NONE;
  cq_attr.size = info->tx_attr->size;
  ret = fi_cq_open(domain, &cq_attr, &txcq, &txcq);
  if (ret) {
    HPS_ERR("fi_cq_open for send %d", ret);
    return ret;
  }

  // create a file descriptor wait cq set
  cq_attr.wait_obj = FI_WAIT_FD;
  cq_attr.wait_cond = FI_CQ_COND_NONE;
  cq_attr.size = info->rx_attr->size;
  ret = fi_cq_open(domain, &cq_attr, &rxcq, &rxcq);
  if (ret) {
    HPS_ERR("fi_cq_open for receive %d", ret);
    return ret;
  }


  if (this->info->ep_attr->type == FI_EP_RDM || this->info->ep_attr->type == FI_EP_DGRAM) {
    if (this->info->domain_attr->av_type != FI_AV_UNSPEC) {
      av_attr.type = info->domain_attr->av_type;
    }

    if (this->options->av_name) {
      av_attr.name = this->options->av_name;
    }
    ret = fi_av_open(this->domain, &this->av_attr, &this->av, NULL);
    if (ret) {
      HPS_ERR("fi_av_open %d", ret);
      return ret;
    }
  }

  return 0;
}

int Connection::AllocateBuffers(void) {
  int ret = 0;
  long alignment = 1;
  Options *opts = this->options;

  tx_size = opts->buf_size / 2;
  if (tx_size > info->ep_attr->max_msg_size) {
    tx_size = info->ep_attr->max_msg_size;
  }
  rx_size = tx_size + hps_utils_rx_prefix_size(this->info);
  tx_size += hps_utils_tx_prefix_size(this->info);
  buf_size = MAX(tx_size, HPS_MAX_CTRL_MSG) + MAX(rx_size, HPS_MAX_CTRL_MSG);

  if (opts->options & HPS_OPT_ALIGN) {
    alignment = sysconf(_SC_PAGESIZE);
    if (alignment < 0) {
      return -errno;
    }
    buf_size += alignment;

    ret = posix_memalign((void **)&buf, (size_t) alignment, buf_size);
    if (ret) {
      HPS_ERR("posix_memalign %d", ret);
      return ret;
    }
  } else {
    buf = (uint8_t *)malloc(buf_size);
    if (!buf) {
      HPS_ERR("No memory");
      return -FI_ENOMEM;
    }
  }
  memset(buf, 0, buf_size);
  rx_buf = buf;
  tx_buf = buf + MAX(rx_size, HPS_MAX_CTRL_MSG);
  tx_buf = (uint8_t *) (((uintptr_t) tx_buf + alignment - 1) & ~(alignment - 1));

  remote_cq_data = hps_utils_init_cq_data(info);

  if (!ft_skip_mr && ((info->mode & FI_LOCAL_MR) ||
                      (info->caps & (FI_RMA | FI_ATOMIC)))) {
    ret = fi_mr_reg(domain, buf, buf_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY, 0, &mr, NULL);
    if (ret) {
      HPS_ERR("fi_mr_reg %d", ret);
      return ret;
    }
  } else {
    mr = &no_mr;
  }

  this->send_buf = new Buffer(tx_buf, tx_size, opts->no_buffers);
  this->recv_buf = new Buffer(rx_buf, rx_size, opts->no_buffers);
  return 0;
}

int Connection::InitEndPoint(struct fid_ep *ep, struct fid_eq *eq) {
  uint64_t flags;
  int ret;
  printf("Init EP\n");

  this->ep = ep;
  if (this->info->ep_attr->type == FI_EP_MSG) {
    HPS_EP_BIND(ep, eq, 0);
  }
  HPS_EP_BIND(ep, av, 0);
  HPS_EP_BIND(ep, txcq, FI_TRANSMIT);
  HPS_EP_BIND(ep, rxcq, FI_RECV);

  ret = hps_utils_get_cq_fd(this->options, txcq, &tx_fd);
  if (ret) {
    HPS_ERR("Failed to get cq fd for transmission");
    return ret;
  }

  ret = hps_utils_get_cq_fd(this->options, rxcq, &rx_fd);
  if (ret) {
    HPS_ERR("Failed to get cq fd for receive");
    return ret;
  }

  flags = !txcq ? FI_SEND : 0;
  if (this->info_hints->caps & (FI_WRITE | FI_READ)) {
    flags |= this->info_hints->caps & (FI_WRITE | FI_READ);
  } else if (this->info_hints->caps & FI_RMA) {
    flags |= FI_WRITE | FI_READ;
  }

  HPS_EP_BIND(ep, txcntr, flags);
  flags = !rxcq ? FI_RECV : 0;
  if (this->info_hints->caps & (FI_REMOTE_WRITE | FI_REMOTE_READ)) {
    flags |= this->info_hints->caps & (FI_REMOTE_WRITE | FI_REMOTE_READ);
  } else if (this->info_hints->caps & FI_RMA) {
    flags |= FI_REMOTE_WRITE | FI_REMOTE_READ;
  }
  HPS_EP_BIND(ep, rxcntr, flags);
  ret = fi_enable(ep);
  if (ret) {
    HPS_ERR("fi_enable %d", ret);
    return ret;
  }
  return 0;
}

int Connection::SetupBuffers() {
  this->rx_seq = 0;
  this->rx_cq_cntr = 0;
  this->tx_cq_cntr = 0;
  this->tx_seq = 0;
  ssize_t ret = 0;
  Buffer *rBuf = this->recv_buf;
  uint32_t noBufs = rBuf->NoOfBuffers();
  HPS_INFO("Head, tail, datahead %" PRId32 " %" PRId32 " %" PRId32 " %" PRIu64 " %" PRIu64, rBuf->Head(), rBuf->Tail(), rBuf->DataHead(), rx_seq, rx_cq_cntr);
  for (uint32_t i = 0; i < noBufs; i++) {
    uint8_t *buf = rBuf->GetBuffer(i);
    ret = PostRX(rBuf->BufferSize(), buf, &rx_ctx);
    if (ret) {
      HPS_ERR("PostRX %d", ret);
      return (int) ret;
    }
    rBuf->SetHead(i);
  }
  HPS_INFO("Head, tail, datahead %" PRId32 " %" PRId32 " %" PRId32 " %" PRIu64 " %" PRIu64, rBuf->Head(), rBuf->Tail(), rBuf->DataHead(), rx_seq, rx_cq_cntr);
  return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
int Connection::SpinForCompletion(struct fid_cq *cq, uint64_t *cur,
                                  uint64_t total, int timeout) {
  struct fi_cq_err_entry comp;
  struct timespec a, b;
  ssize_t ret;

  if (timeout >= 0) {
    clock_gettime(CLOCK_MONOTONIC, &a);
  }

  while (total - *cur > 0) {
    ret = fi_cq_read(cq, &comp, 1);
    if (ret > 0) {
      if (timeout >= 0) {
        clock_gettime(CLOCK_MONOTONIC, &a);
      }

      (*cur)++;
    } else if (ret < 0 && ret != -FI_EAGAIN) {
      return (int) ret;
    } else if (timeout >= 0) {
      clock_gettime(CLOCK_MONOTONIC, &b);
      if ((b.tv_sec - a.tv_sec) > timeout) {
        fprintf(stderr, "%ds timeout expired\n", timeout);
        return -FI_ENODATA;
      }
    }
  }

  return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
int Connection::WaitForCompletion(struct fid_cq *cq, uint64_t *cur,
                                  uint64_t total, int timeout) {
  struct fi_cq_err_entry comp;
  ssize_t ret;
  while (total - *cur > 0) {
    ret = fi_cq_sread(cq, &comp, 1, NULL, timeout);
    if (ret > 0) {
      (*cur)++;
    }
    else if (ret < 0 && ret != -FI_EAGAIN) {
      return (int) ret;
    }
  }
  return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
int Connection::FDWaitForComp(struct fid_cq *cq, uint64_t *cur,
                              uint64_t total, int timeout) {
  struct fi_cq_err_entry comp;
  struct fid *fids[1];
  int fd, ret;

  fd = cq == txcq ? tx_fd : rx_fd;
  fids[0] = &cq->fid;
  while (total - *cur > 0) {
    ret = fi_trywait(fabric, fids, 1);
    if (ret == FI_SUCCESS) {
      ret = hps_utils_poll_fd(fd, timeout);
      if (ret && ret != -FI_EAGAIN) {
        return ret;
      }
    }
    ssize_t cq_ret = fi_cq_read(cq, &comp, 1);
    if (cq_ret > 0) {
      (*cur)++;
    } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
      return (int) cq_ret;
    }
  }
  return 0;
}

int Connection::GetCQComp(struct fid_cq *cq, uint64_t *cur,
                          uint64_t total, int timeout) {
  int ret;

  switch (this->options->comp_method) {
    case HPS_COMP_SREAD:
      ret = WaitForCompletion(cq, cur, total, timeout);
      break;
    case HPS_COMP_WAIT_FD:
      ret = FDWaitForComp(cq, cur, total, timeout);
      break;
    default:
      ret = SpinForCompletion(cq, cur, total, timeout);
      break;
  }

  if (ret) {
    if (ret == -FI_EAVAIL) {
      ret = hps_utils_cq_readerr(cq);
      (*cur)++;
    } else {
      HPS_ERR("ft_get_cq_comp %d", ret);
    }
  }
  return ret;
}

int Connection::GetRXComp(uint64_t total) {
  int ret = FI_SUCCESS;

  if (rxcq) {
    ret = GetCQComp(rxcq, &rx_cq_cntr, total, timeout);
  } else if (rxcntr) {
    while (fi_cntr_read(rxcntr) < total) {
      ret = fi_cntr_wait(rxcntr, total, timeout);
      if (ret) {
        HPS_ERR("fi_cntr_wait %d", ret);
      } else {
        break;
      }
    }
  } else {
    HPS_ERR("Trying to get a RX completion when no RX CQ or counter were opened");
    ret = -FI_EOTHER;
  }
  return ret;
}

int Connection::GetTXComp(uint64_t total) {
  int ret;

  if (txcq) {
    ret = GetCQComp(txcq, &tx_cq_cntr, total, -1);
  } else if (txcntr) {
    ret = fi_cntr_wait(txcntr, total, -1);
    if (ret) {
      HPS_ERR("fi_cntr_wait %d", ret);
    }
  } else {
    HPS_ERR("Trying to get a TX completion when no TX CQ or counter were opened");
    ret = -FI_EOTHER;
  }
  return ret;
}

ssize_t Connection::PostTX(size_t size, struct fi_context* ctx) {
  if (info_hints->caps & FI_TAGGED) {
    HPS_POST(fi_tsend, GetTXComp, tx_seq, "transmit", ep,
            tx_buf, size + hps_utils_tx_prefix_size(info), fi_mr_desc(mr),
            this->remote_fi_addr, tx_seq, ctx);
  } else {
    HPS_POST(fi_send, GetTXComp, tx_seq, "transmit", ep,
            tx_buf,	size + hps_utils_tx_prefix_size(info), fi_mr_desc(mr),
            this->remote_fi_addr, ctx);
  }
  return 0;
}

ssize_t Connection::PostTX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  if (info_hints->caps & FI_TAGGED) {
    HPS_POST(fi_tsend, GetTXComp, tx_seq, "transmit", ep,
             buf, size + hps_utils_tx_prefix_size(info), fi_mr_desc(mr),
             this->remote_fi_addr, tx_seq, ctx);
  } else {
    HPS_POST(fi_send, GetTXComp, tx_seq, "transmit", ep,
             buf,	size + hps_utils_tx_prefix_size(info), fi_mr_desc(mr),
             this->remote_fi_addr, ctx);
  }
  return 0;
}

ssize_t Connection::TX(size_t size) {
  ssize_t ret;

  if (hps_utils_check_opts(options, HPS_OPT_VERIFY_DATA | HPS_OPT_ACTIVE)) {
    hps_utils_fill_buf((char *) tx_buf + hps_utils_tx_prefix_size(info), size);
  }

  ret = PostTX(size, &this->tx_ctx);
  if (ret) {
    return ret;
  }

  ret = GetTXComp(tx_seq);
  printf("Waiting tx %ld with size %ld\n", tx_seq, size);
  return ret;
}

ssize_t Connection::PostRX(size_t size, struct fi_context* ctx) {
  if (info_hints->caps & FI_TAGGED) {
    HPS_POST(fi_trecv, GetRXComp, rx_seq, "receive", this->ep, rx_buf,
            MAX(size, HPS_MAX_CTRL_MSG) + hps_utils_rx_prefix_size(info),
            fi_mr_desc(mr), 0, rx_seq, 0, ctx);
  } else {
    HPS_POST(fi_recv, GetRXComp, rx_seq, "receive", this->ep, rx_buf,
            MAX(size, HPS_MAX_CTRL_MSG) + hps_utils_rx_prefix_size(info),
            fi_mr_desc(mr),	0, ctx);
  }
  return 0;
}

ssize_t Connection::PostRX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  if (info_hints->caps & FI_TAGGED) {
    HPS_POST(fi_trecv, GetRXComp, rx_seq, "receive", this->ep, buf,
             MAX(size, HPS_MAX_CTRL_MSG) + hps_utils_rx_prefix_size(info),
             fi_mr_desc(mr), 0, rx_seq, 0, ctx);
  } else {
    HPS_POST(fi_recv, GetRXComp, rx_seq, "receive", this->ep, buf,
             MAX(size, HPS_MAX_CTRL_MSG) + hps_utils_rx_prefix_size(info),
             fi_mr_desc(mr),	0, ctx);
    HPS_INFO("Finished posting buffer with size %ld", size);
  }
  return 0;
}

ssize_t Connection::RX(size_t size) {
  ssize_t ret;
  printf("Waiting rx %ld with size %ld\n", rx_cq_cntr, size);
  ret = GetRXComp(rx_cq_cntr + 1);
  if (ret)
    return ret;

  if (hps_utils_check_opts(options, HPS_OPT_VERIFY_DATA | HPS_OPT_ACTIVE)) {
    ret = hps_utils_check_buf((char *) rx_buf + hps_utils_rx_prefix_size(info), (int) size);
    if (ret)
      return ret;
  }

  ret = PostRX(this->rx_size, &this->rx_ctx);
  return ret;
}

ssize_t Connection::PostRMA(enum hps_rma_opcodes op, size_t size) {
  struct fi_rma_iov *remote = &this->remote;
  switch (op) {
    case HPS_RMA_WRITE:
      HPS_POST(fi_write, GetTXComp, tx_seq, "fi_write", ep, tx_buf,
              size, fi_mr_desc(mr), remote_fi_addr,
              remote->addr, remote->key, ep);
      break;
    case HPS_RMA_WRITEDATA:
      HPS_POST(fi_writedata, GetTXComp, tx_seq, "fi_writedata", ep,
              tx_buf, size, fi_mr_desc(mr),
              remote_cq_data,	remote_fi_addr,	remote->addr,
              remote->key, ep);
      break;
    case HPS_RMA_READ:
      HPS_POST(fi_read, GetTXComp, tx_seq, "fi_read", ep, rx_buf,
              size, fi_mr_desc(mr), remote_fi_addr,
              remote->addr, remote->key, ep);
      break;
    default:
      HPS_ERR("Unknown RMA op type");
      return EXIT_FAILURE;
  }

  return 0;
}

ssize_t Connection::PostRMA(enum hps_rma_opcodes op, size_t size, void *buf) {
  switch (op) {
    case HPS_RMA_WRITE:
      HPS_POST(fi_write, GetTXComp, tx_seq, "fi_write", ep, buf,
              size, fi_mr_desc(mr), remote_fi_addr,
              remote.addr, remote.key, ep);
      break;
    case HPS_RMA_WRITEDATA:
      HPS_POST(fi_writedata, GetTXComp, tx_seq, "fi_writedata", ep,
              buf, size, fi_mr_desc(mr),
              remote_cq_data,	remote_fi_addr,	remote.addr,
              remote.key, ep);
      break;
    case HPS_RMA_READ:
      HPS_POST(fi_read, GetTXComp, tx_seq, "fi_read", ep, buf,
              size, fi_mr_desc(mr), remote_fi_addr,
              remote.addr, remote.key, ep);
      break;
    default:
      HPS_ERR("Unknown RMA op type");
      return EXIT_FAILURE;
  }

  return 0;
}

ssize_t Connection::RMA(enum hps_rma_opcodes op, size_t size) {
  ssize_t ret;

  ret = PostRMA(op, size);
  if (ret)
    return ret;

  if (op == HPS_RMA_WRITEDATA) {
    ret = RX(0);
    if (ret)
      return ret;
  }

  ret = GetTXComp(tx_seq);
  if (ret)
    return ret;

  return 0;
}

int Connection::ExchangeServerKeys() {
  struct fi_rma_iov *peer_iov = &this->remote;
  struct fi_rma_iov *rma_iov;
  ssize_t ret;
  HPS_INFO("Exchange key");
  print_short_info(this->info);
  HPS_INFO("Exchange key 2 %d\n", hps_utils_tx_prefix_size(this->info));
  ret = GetRXComp(rx_seq);
  if (ret) {
    HPS_ERR("Failed to RX Completion");
    return (int) ret;
  }
  HPS_INFO("Exchange key 3 \n");
  rma_iov = (fi_rma_iov *)(rx_buf + hps_utils_rx_prefix_size(info));
  *peer_iov = *rma_iov;
  HPS_INFO("Exchange key 4 \n");
  ret = PostRX(rx_size, &rx_ctx);
  if (ret) {
    HPS_ERR("Failed to post RX");
    return (int) ret;
  }
  HPS_INFO("Received keys");
  rma_iov = (fi_rma_iov *)(tx_buf + hps_utils_tx_prefix_size(info));
  rma_iov->addr = info->domain_attr->mr_mode == FI_MR_SCALABLE ?
                  0 : (uintptr_t) rx_buf + hps_utils_rx_prefix_size(info);
  rma_iov->key = fi_mr_key(mr);
  ret = TX(sizeof *rma_iov);
  if (ret) {
    HPS_ERR("Failed to TX");
    return (int) ret;
  }
  HPS_INFO("Sent keys");
  return (int) ret;
}

int Connection::ExchangeClientKeys() {
  struct fi_rma_iov *peer_iov = &this->remote;
  struct fi_rma_iov *rma_iov;
  ssize_t ret;
  HPS_INFO("Exchange key");
  print_short_info(this->info);
  HPS_INFO("Exchange key 2 %d\n", hps_utils_tx_prefix_size(this->info));
  rma_iov = (fi_rma_iov *)(tx_buf + hps_utils_tx_prefix_size(info));
  rma_iov->addr = info->domain_attr->mr_mode == FI_MR_SCALABLE ?
                  0 : (uintptr_t) rx_buf + hps_utils_rx_prefix_size(info);
  rma_iov->key = fi_mr_key(mr);
  ret = TX(sizeof *rma_iov);
  HPS_INFO("Sent keys");
  if (ret) {
    HPS_ERR("Failed to TX");
    return (int) ret;
  }

  ret = GetRXComp(rx_seq);
  if (ret) {
    HPS_ERR("Failed to get rx completion");
    return (int) ret;
  }

  rma_iov = (fi_rma_iov *)(rx_buf + hps_utils_rx_prefix_size(info));
  *peer_iov = *rma_iov;
  ret = PostRX(rx_size, &rx_ctx);
  if (ret) {
    HPS_ERR("Failed to post RX");
    return (int) ret;
  }
  HPS_INFO("Received keys");
  return (int) ret;
}

int Connection::ClientSync() {
  ssize_t ret;
  ret = TX(1);
  if (ret) {
    return (int) ret;
  }

  ret = RX(1);
  return (int) ret;
}

int Connection::ServerSync() {
  ssize_t ret;
  ret = RX(1);
  if (ret) {
    return (int) ret;
  }

  ret = TX(1);
  return (int) ret;
}

int Connection::SendCompletions(uint64_t min, uint64_t max) {
  ssize_t ret = 0;
  ssize_t cq_read = 0;
  struct fi_cq_err_entry comp;
  struct timespec a, b;

  if (txcq) {
    if (timeout >= 0) {
      clock_gettime(CLOCK_MONOTONIC, &a);
    }

    HPS_INFO("CQ_Read for txcq tx_seq=%ld tx_cntr=%ld", tx_seq, tx_cq_cntr);
    while (tx_cq_cntr < max) {
      cq_read = fi_cq_read(txcq, &comp, 1);

      if (cq_read > 0) {
        if (timeout >= 0) {
          clock_gettime(CLOCK_MONOTONIC, &a);
        }
        tx_cq_cntr += cq_read;
        if (tx_cq_cntr >= max) {
          HPS_INFO("Done completion %ld", rx_cq_cntr);
          break;
        }
      } else if (cq_read < 0 && cq_read != -FI_EAGAIN) {
        HPS_INFO("Failed %ld %ld", rx_cq_cntr, cq_read);
        return (int) cq_read;
      } else if (min <= tx_cq_cntr && cq_read == -FI_EAGAIN) {
        // we have read enough to return
        HPS_INFO("Done completion %ld", rx_cq_cntr);
        break;
      } else if (timeout >= 0) {
        clock_gettime(CLOCK_MONOTONIC, &b);
        if ((b.tv_sec - a.tv_sec) > timeout) {
          HPS_ERR("%ds timeout expired", timeout);
          return -FI_ENODATA;
        }
      }
    }
  } else if (txcntr) {
    ret = fi_cntr_wait(txcntr, min, -1);
    if (ret) {
      HPS_ERR("fi_cntr_wait %ld", ret);
    }
  } else {
    HPS_ERR("Trying to get a TX completion when no TX CQ or counter were opened");
    ret = -FI_EOTHER;
  }
  return (int) ret;
}

/**
 * Receive completions at least 'total' completions and until rx_seq
 * completions
 */
int Connection::ReceiveCompletions(uint64_t min, uint64_t max) {
  ssize_t ret = FI_SUCCESS;
  ssize_t cq_read = 0;
  struct fi_cq_err_entry comp;
  struct timespec a, b;
  // in case we are using completion queue
  if (rxcq) {
    if (timeout >= 0) {
      clock_gettime(CLOCK_MONOTONIC, &a);
    }

    HPS_INFO("CQ_Read for rxcq rx_seq=%ld rx_cntr=%ld", rx_seq, rx_cq_cntr);
    while (rx_cq_cntr < max	) {
      cq_read = fi_cq_read(rxcq, &comp, 1);
      if (cq_read > 0) {
        if (timeout >= 0) {
          clock_gettime(CLOCK_MONOTONIC, &a);
        }
        rx_cq_cntr += cq_read;
        // we've reached max
        if (rx_cq_cntr >= max) {
          HPS_INFO("Done completion %ld", rx_cq_cntr);
          break;
        }
      } else if (cq_read < 0 && cq_read != -FI_EAGAIN) {
        ret = cq_read;
        HPS_INFO("Failed %ld %ld", rx_cq_cntr, cq_read);
        break;
      } else if (min <= rx_cq_cntr && cq_read == -FI_EAGAIN) {
        // we have read enough to return
        HPS_INFO("Done looping %ld %ld", rx_cq_cntr, cq_read);
        break;
      } else if (timeout >= 0) {
        clock_gettime(CLOCK_MONOTONIC, &b);
        if ((b.tv_sec - a.tv_sec) > timeout) {
          HPS_ERR("%ds timeout expired", timeout);
          ret = -FI_ENODATA;
          break;
        }
      }
    }

    if (ret) {
      if (ret == -FI_EAVAIL) {
        ret = hps_utils_cq_readerr(rxcq);
        rx_cq_cntr++;
      } else {
        HPS_ERR("ft_get_cq_comp %ld", ret);
      }
    }
    return 0;
  } else {
    HPS_ERR("Trying to get a RX completion when no RX CQ or counter were opened");
    ret = -FI_EOTHER;
  }
  return (int) ret;
}

int Connection::Receive() {
  int ret;
  Buffer *sbuf = this->recv_buf;
  uint32_t data_head;
  uint32_t buffers = sbuf->NoOfBuffers();
  HPS_INFO("1 Data head, tail and head at %" PRIu32 " %" PRIu32 " %" PRIu32 "", sbuf->DataHead(), sbuf->Tail(), sbuf->Head());
  // now wait until a receive is completed
  HPS_INFO("Receive with %ld %ld", rx_cq_cntr + 1, rx_seq);
  ret = ReceiveCompletions(rx_cq_cntr + 1, rx_seq);
  if (ret < 0) {
    HPS_ERR("Failed to retrieve");
    return 1;
  }
  // ok a receive is completed
  // mark the buffers with the data
  // now update the buffer according to the rx_cq_cntr and rx_cq
  data_head = (uint32_t) ((rx_cq_cntr - 1) % buffers);
  sbuf->SetDataHead(data_head);
  HPS_INFO("2 Data head, tail and head at dataHead=%" PRIu32 " tail=%" PRIu32 " head=%" PRIu32 " rx_cq_cntr=%" PRId64, sbuf->DataHead(), sbuf->Tail(), sbuf->Head(), rx_cq_cntr);
  return 0;
}

bool Connection::DataAvailableForRead() {
  Buffer *sbuf = this->recv_buf;
  return sbuf->DataHead() != sbuf->Tail();
}

int Connection::ReadData(uint8_t *buf, uint32_t size, uint32_t *read) {
  ssize_t ret = 0;

  // go through the buffers
  Buffer *rbuf = this->recv_buf;
  rbuf->acquireLock();
  ret = rbuf->ReadData(buf, size, read);
  // now lock the buffer
  uint32_t tail = rbuf->Tail();
  uint32_t head = rbuf->Head();
  uint32_t next_head = (head + 1) % rbuf->NoOfBuffers();
  while (tail != next_head) {
    uint8_t *send_buf = rbuf->GetBuffer(head);
    ret = PostRX(rbuf->BufferSize(), send_buf, &this->rx_ctx);
    if (ret) {
      HPS_ERR("Failed to post the receive buffer");
      return (int) ret;
    }
    rbuf->IncrementHead(1);
    head = rbuf->Head();
    next_head = (head + 1) % rbuf->NoOfBuffers();
  }
  rbuf->releaseLock();
  return 0;
}

int Connection::WriteBuffers() {
  int ret;
  Buffer *sbuf = this->send_buf;
  uint32_t data_head;
  uint32_t buffers = sbuf->NoOfBuffers();
  // now wait until a receive is completed
  HPS_INFO("Write with %ld %ld", tx_cq_cntr + 1, tx_seq);
  ret = SendCompletions(tx_cq_cntr + 1, tx_seq);
  if (ret < 0) {
    HPS_ERR("Failed to send complete");
    return 1;
  }
  // ok a receive is completed
  // mark the buffers with the data
  // now update the buffer according to the rx_cq_cntr and rx_cq
  data_head = (uint32_t) (tx_cq_cntr % buffers);
  sbuf->SetTail(data_head);
  return 0;
}

int Connection::WriteData(uint8_t *buf, uint32_t size) {
  int ret;
  HPS_INFO("Init writing");
  // first lets get the available buffer
  Buffer *sbuf = this->send_buf;
  sbuf->acquireLock();
  // now determine the buffer no to use
  uint32_t sent_size = 0;
  uint32_t current_size = 0;
  uint32_t head = 0;
  uint32_t error_count = 0;

  uint32_t buf_size = sbuf->BufferSize() - 4;
  // we need to send everything by using the buffers available
  while (sent_size < size) {
    uint64_t free_space = sbuf->GetFreeSpace();
    // we have space in the buffers
    if (free_space > 0) {
      HPS_INFO("Free space %d", free_space);
      head = sbuf->Head();
      uint8_t *current_buf = sbuf->GetBuffer(head);
      // now lets copy from send buffer to current buffer chosen
      current_size = (size - sent_size) < buf_size ? size - sent_size : buf_size;
      //memcpy(current_buf, &current_size, sizeof(uint32_t));
      uint32_t *length = (uint32_t *) current_buf;
      *length = current_size;
      HPS_INFO("Sending length %"
                   PRIu32, *((uint32_t *) current_buf));
      memcpy(current_buf + sizeof(uint32_t), buf + sent_size, current_size);
      // send the current buffer
      if (!PostTX(current_size + sizeof(uint32_t), current_buf, &this->tx_ctx)) {
        sent_size += current_size;
        // increment the head
        sbuf->IncrementHead(1);
      } else {
        error_count++;
        if (error_count > MAX_ERRORS) {
          HPS_ERR("Failed to send the buffer completely. sent %d", sent_size);
          goto err;
        }
      }
    } else {
      sbuf->waitFree();
    }
  }
  sbuf->releaseLock();
  return 0;

  err:
    sbuf->releaseLock();
    return 1;
}

int Connection::TransmitComplete() {
  HPS_INFO("Transmit complete");
  struct fi_cq_err_entry comp;
  int ret;
  // lets get the number of completions
  size_t max_completions = tx_seq - tx_cq_cntr;
  // we can expect up to this
  ssize_t cq_ret = fi_cq_read(txcq, &comp, max_completions);
  if (cq_ret > 0) {
    this->tx_cq_cntr += cq_ret;
  } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
    // okay we have an error
    if (cq_ret == -FI_EAVAIL) {
      cq_ret = hps_utils_cq_readerr(txcq);
      this->tx_cq_cntr++;
    } else {
      HPS_ERR("ft_get_cq_comp %d", cq_ret);
      return (int) cq_ret;
    }
  }
  // increment the buffer
  if (this->send_buf->IncrementTail((uint32_t) cq_ret)) {
    HPS_ERR("Failed to increment buffer data pointer");
    return 1;
  }
  return 0;
}

int Connection::ReceiveComplete() {
  HPS_INFO("Receive complete");
  struct fi_cq_err_entry comp;
  int ret;
  // lets get the number of completions
  size_t max_completions = rx_seq - rx_cq_cntr;
  // we can expect up to this
  ssize_t cq_ret = fi_cq_read(rxcq, &comp, max_completions);
  if (cq_ret > 0) {
    this->rx_cq_cntr += cq_ret;
  } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
    // okay we have an error
    if (cq_ret == -FI_EAVAIL) {
      cq_ret = hps_utils_cq_readerr(rxcq);
      this->rx_cq_cntr++;
    } else {
      HPS_ERR("ft_get_cq_comp %d", cq_ret);
      return (int) cq_ret;
    }
  }

  if (this->recv_buf->IncrementDataHead((uint32_t) cq_ret)) {
    HPS_ERR("Failed to increment buffer data pointer");
    return 1;
  }

  return 0;
}

int Connection::Ready(int fd) {
  HPS_INFO("Connection ready %d", fd);
  if (fd == tx_fd) {
    TransmitComplete();
  }

  if (fd == rx_fd) {
    ReceiveComplete();
  }
  return 0;
}

int Connection::Print() {
  HPS_INFO("Printing %d %d", tx_fd, rx_fd);
  return 1;
}

Connection::~Connection() {

}
