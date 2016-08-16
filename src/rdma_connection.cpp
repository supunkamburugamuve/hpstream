#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <cinttypes>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>
#include <arpa/inet.h>

#include "rdma_connection.h"

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


Connection::Connection(RDMAOptions *opts, struct fi_info *info_hints, struct fi_info *info,
                       struct fid_fabric *fabric, struct fid_domain *domain, struct fid_eq *eq) {

  print_short_info(info);

  this->options = opts;
  this->info = info;
  this->info_hints = info_hints;
  this->fabric = fabric;
  this->domain = domain;

  this->txcq = NULL;
  this->rxcq = NULL;

  this->tx_loop.callback = this;
  this->rx_loop.callback = this;
  this->tx_loop.event = CQ_TRANSMIT;
  this->rx_loop.event = CQ_READ;

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
  HPS_CLOSE_FID(av);
  HPS_CLOSE_FID(domain);
  HPS_CLOSE_FID(waitset);
  HPS_CLOSE_FID(fabric);
}

RDMABuffer * Connection::ReceiveBuffer() {
  return this->recv_buf;
}

RDMABuffer * Connection::SendBuffer() {
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
  RDMAOptions *opts = this->options;

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

  this->send_buf = new RDMABuffer(tx_buf, tx_size, opts->no_buffers);
  this->recv_buf = new RDMABuffer(rx_buf, rx_size, opts->no_buffers);
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
  this->tx_loop.fid = tx_fd;

  ret = hps_utils_get_cq_fd(this->options, rxcq, &rx_fd);
  if (ret) {
    HPS_ERR("Failed to get cq fd for receive");
    return ret;
  }
  this->rx_loop.fid = rx_fd;

  flags = !txcq ? FI_SEND : 0;
  if (this->info_hints->caps & (FI_WRITE | FI_READ)) {
    flags |= this->info_hints->caps & (FI_WRITE | FI_READ);
  } else if (this->info_hints->caps & FI_RMA) {
    flags |= FI_WRITE | FI_READ;
  }

  flags = !rxcq ? FI_RECV : 0;
  if (this->info_hints->caps & (FI_REMOTE_WRITE | FI_REMOTE_READ)) {
    flags |= this->info_hints->caps & (FI_REMOTE_WRITE | FI_REMOTE_READ);
  } else if (this->info_hints->caps & FI_RMA) {
    flags |= FI_REMOTE_WRITE | FI_REMOTE_READ;
  }
  ret = fi_enable(ep);
  if (ret) {
    HPS_ERR("fi_enable %d", ret);
    return ret;
  }
  return 0;
}

int Connection::  SetupBuffers() {
  this->rx_seq = 0;
  this->rx_cq_cntr = 0;
  this->tx_cq_cntr = 0;
  this->tx_seq = 0;
  ssize_t ret = 0;
  RDMABuffer *rBuf = this->recv_buf;
  uint32_t noBufs = rBuf->NoOfBuffers();
  HPS_INFO("base, filled submitted %ld %ld %ld", rBuf->Base(), rBuf->GetFilledBuffers(), rBuf->GetSubmittedBuffers());
  for (uint32_t i = 0; i < noBufs; i++) {
    uint8_t *buf = rBuf->GetBuffer(i);
    ret = PostRX(rBuf->BufferSize(), buf, &rx_ctx);
    if (ret) {
      HPS_ERR("PostRX %d", ret);
      return (int) ret;
    }
    rBuf->IncrementSubmitted(1);
  }
  HPS_INFO("base, filled submitted % " PRId32 "% " PRId32 "% " PRId32, rBuf->Base(), rBuf->GetFilledBuffers(), rBuf->GetSubmittedBuffers());
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
  } else {
    HPS_ERR("Trying to get a TX completion when no TX CQ or counter were opened");
    ret = -FI_EOTHER;
  }
  return ret;
}

ssize_t Connection::PostTX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  int timeout_save;
  ssize_t ret, rc;

  while (1) {
    ret = fi_send(this->ep, buf, size + hps_utils_rx_prefix_size(info), fi_mr_desc(mr),	0, ctx);
    if (!ret)
      break;

    if (ret != -FI_EAGAIN) {
      HPS_ERR("%s %d", "receive", ret);
      return ret;
    }

    timeout_save = timeout;
    timeout = 0;
    rc = GetTXComp(tx_cq_cntr + 1);
    if (rc && rc != -FI_EAGAIN) {
      HPS_ERR("Failed to get %s completion\n", "receive");
      return rc;
    }
    timeout = timeout_save;
  }
  tx_seq++;
  return 0;
}

ssize_t Connection::PostRX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  int timeout_save;
  ssize_t ret, rc;

  while (1) {
    ret = fi_recv(this->ep, buf, size + hps_utils_rx_prefix_size(info), fi_mr_desc(mr),	0, ctx);
    if (!ret)
      break;

    if (ret != -FI_EAGAIN) {
      HPS_ERR("%s %d", "receive", ret);
      return ret;
    }

    timeout_save = timeout;
    timeout = 0;
    rc = GetRXComp(rx_cq_cntr + 1);
    if (rc && rc != -FI_EAGAIN) {
      HPS_ERR("Failed to get %s completion\n", "receive");
      return rc;
    }
    timeout = timeout_save;
  }
  rx_seq++;
//  HPS_INFO("Finished posting buffer with size %ld", size);
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
  ret = PostRX(rx_size, rx_buf, &rx_ctx);
  if (ret) {
    HPS_ERR("Failed to post RX");
    return (int) ret;
  }
  HPS_INFO("Received keys");
  rma_iov = (fi_rma_iov *)(tx_buf + hps_utils_tx_prefix_size(info));
  rma_iov->addr = info->domain_attr->mr_mode == FI_MR_SCALABLE ?
                  0 : (uintptr_t) rx_buf + hps_utils_rx_prefix_size(info);
  rma_iov->key = fi_mr_key(mr);
  ret = PostTX(sizeof *rma_iov, tx_buf, &tx_ctx);
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
  ret = PostTX(sizeof *rma_iov, tx_buf, &tx_ctx);
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
  ret = PostRX(rx_size, rx_buf, &rx_ctx);
  if (ret) {
    HPS_ERR("Failed to post RX");
    return (int) ret;
  }
  HPS_INFO("Received keys");
  return (int) ret;
}


bool Connection::DataAvailableForRead() {
  RDMABuffer *sbuf = this->recv_buf;
  return sbuf->GetFilledBuffers() > 0;
}

int Connection::ReadData(uint8_t *buf, uint32_t size, uint32_t *read) {
  ssize_t ret = 0;
  uint32_t base;
  uint32_t submittedBuffers;
  uint32_t noOfBuffers;
  uint32_t index = 0;
  // go through the buffers
  RDMABuffer *rbuf = this->recv_buf;
  // now lock the buffer
  rbuf->acquireLock();

  if (rbuf->GetFilledBuffers() == 0) {
    *read = 0;
    return 0;
  }
  uint32_t tail = rbuf->Base();
  uint32_t buffers_filled = rbuf->GetFilledBuffers();
  uint32_t current_read_indx = rbuf->CurrentReadIndex();
  // need to copy
  uint32_t need_copy = 0;
  // number of bytes copied
  uint32_t read_size = 0;
//  HPS_INFO("Reading, base= %d, dataHead= %d", tail, buffers_filled);
  while (read_size < size &&  buffers_filled > 0) {
    uint8_t *b = rbuf->GetBuffer(tail);
    uint32_t *r;
    // first read the amount of data in the buffer
    r = (uint32_t *) b;
    // now lets see how much data we need to copy from this buffer
    need_copy = (*r) - current_read_indx;
    // now lets see how much we can copy
    uint32_t can_copy = 0;
    uint32_t tmp_index = current_read_indx;
//    HPS_INFO("Copy size=%" PRIu32 " read_size=%" PRIu32 " need_copy=%" PRIu32 " r=%" PRIu32 " read_idx=%" PRIu32, size, read_size, need_copy, *r, current_read_indx);
    // we can copy everything from this buffer
    if (size - read_size >= need_copy) {
//      HPS_INFO("Moving base");
      can_copy = need_copy;
      current_read_indx = 0;
      // advance the base pointer
      rbuf->IncrementTail(1);
      buffers_filled--;
      tail = rbuf->Base();
    } else {
//      HPS_INFO("Not Moving base");
      // we cannot copy everything from this buffer
      can_copy = size - read_size;
      current_read_indx += can_copy;
    }
    // next copy the buffer
    memcpy(buf + read_size, b + sizeof(uint32_t) + tmp_index, can_copy);
    // now update
//    HPS_INFO("Reading, base= %d, dataHead= %d read_size=%" PRId32, tail, buffers_filled, read_size);
    read_size += can_copy;
  }

  *read = read_size;

  base = rbuf->Base();
  submittedBuffers = rbuf->GetSubmittedBuffers();
  noOfBuffers = rbuf->NoOfBuffers();
  while (submittedBuffers < noOfBuffers) {
    index = (base + submittedBuffers) % noOfBuffers;
    uint8_t *send_buf = rbuf->GetBuffer(index);
//    HPS_INFO("Posting buffer with index %" PRId32, index);
    ret = PostRX(rbuf->BufferSize(), send_buf, &this->rx_ctx);
    if (ret) {
      HPS_ERR("Failed to post the receive buffer");
      rbuf->releaseLock();
      return (int) ret;
    }
    rbuf->IncrementSubmitted(1);
    submittedBuffers++;
  }
  rbuf->releaseLock();
  return 0;
}

int Connection::WriteData(uint8_t *buf, uint32_t size) {
  int ret;
//  HPS_INFO("Init writing");
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  sbuf->acquireLock();
  // now determine the buffer no to use
  uint32_t sent_size = 0;
  uint32_t current_size = 0;
  uint32_t head = 0;
  uint32_t error_count = 0;

  uint32_t buf_size = sbuf->BufferSize() - 4;
  // we need to send everything by using the buffers available
  while (sent_size < size) {
    uint64_t free_space = sbuf->GetAvailableWriteSpace();
    // we have space in the buffers
    if (free_space > 0) {
      // HPS_INFO("base, filled submitted % " PRId32 "% " PRId32 "% " PRId32, sbuf->Base(), sbuf->GetFilledBuffers(), sbuf->GetSubmittedBuffers());
//      HPS_INFO("Free space %d", free_space);
      head = sbuf->NextWriteIndex();
      uint8_t *current_buf = sbuf->GetBuffer(head);
      // now lets copy from send buffer to current buffer chosen
      current_size = (size - sent_size) < buf_size ? size - sent_size : buf_size;
      uint32_t *length = (uint32_t *) current_buf;
      *length = current_size;
//      HPS_INFO("Sending length %"
//                   PRIu32, *((uint32_t *) current_buf));
      memcpy(current_buf + sizeof(uint32_t), buf + sent_size, current_size);
      sbuf->IncrementFilled(1);
      // send the current buffer
      if (!PostTX(current_size + sizeof(uint32_t), current_buf, &this->tx_ctx)) {
        sent_size += current_size;
        // increment the head
        sbuf->IncrementSubmitted(1);
//        HPS_ERR("Posting....");
      } else {
        HPS_ERR("Failed to post");
        error_count++;
        if (error_count > MAX_ERRORS) {
          HPS_ERR("Failed to send the buffer completely. sent %d", sent_size);
          goto err;
        }
      }
    } else {
//      HPS_INFO("Waiting...");
      sbuf->waitFree();
    }
  }
//  HPS_INFO("base, filled submitted % " PRId32 "% " PRId32 "% " PRId32, sbuf->Base(), sbuf->GetFilledBuffers(), sbuf->GetSubmittedBuffers());
  sbuf->releaseLock();
  return 0;

  err:
    sbuf->releaseLock();
    return 1;
}

int Connection::TransmitComplete() {
  struct fi_cq_err_entry comp;
  int ret;
  this->send_buf->acquireLock();
  // lets get the number of completions
  size_t max_completions = tx_seq - tx_cq_cntr;
  // we can expect up to this
  max_completions = max_completions == 0 ? 0 : max_completions;
//  HPS_INFO("Transmit complete max_completions=%ld tx_seq=%ld tx_cq_cntr=%ld rx_seq=%ld rx_cq_cntr=%ld", max_completions, tx_seq, tx_cq_cntr, rx_seq, rx_cq_cntr);
  ssize_t cq_ret = fi_cq_read(txcq, &comp, max_completions);
  if (cq_ret > 0) {
    this->tx_cq_cntr += cq_ret;
//    HPS_INFO("Increment transmit tail %ld", cq_ret);
    if (this->send_buf->IncrementTail((uint32_t) cq_ret)) {
      HPS_ERR("Failed to increment buffer data pointer");
      this->send_buf->releaseLock();
      return 1;
    }
  } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
    // okay we have an error
    if (cq_ret == -FI_EAVAIL) {
      HPS_INFO("Error receive %ld", cq_ret);
      cq_ret = hps_utils_cq_readerr(txcq);
      this->tx_cq_cntr++;
    } else {
      HPS_ERR("ft_get_cq_comp %d", cq_ret);
      this->send_buf->releaseLock();
      return (int) cq_ret;
    }
  }
  this->send_buf->releaseLock();
  return 0;
}

int Connection::ReceiveComplete() {
  struct fi_cq_err_entry comp;
  // lets get the number of completions
  this->recv_buf->acquireLock();
  size_t max_completions = rx_seq - rx_cq_cntr;
  // we can expect up to this
  max_completions = max_completions == 0 ? 0 : max_completions;
//  HPS_INFO("Receive complete max_completions=%ld tx_seq=%ld tx_cq_cntr=%ld rx_seq=%ld rx_cq_cntr=%ld", max_completions, tx_seq, tx_cq_cntr, rx_seq, rx_cq_cntr);
  ssize_t cq_ret = fi_cq_read(rxcq, &comp, max_completions);
  if (cq_ret > 0) {
    this->rx_cq_cntr += cq_ret;
    if (this->recv_buf->IncrementFilled((uint32_t) cq_ret)) {
      HPS_ERR("Failed to increment buffer data pointer");
      this->recv_buf->releaseLock();
      return 1;
    }
//    HPS_INFO("Incremented read filled %ld %ld", this->recv_buf->GetFilledBuffers(), cq_ret);
  } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
    // okay we have an error
    if (cq_ret == -FI_EAVAIL) {
      HPS_INFO("Error receive %ld", cq_ret);
      cq_ret = hps_utils_cq_readerr(rxcq);
      this->rx_cq_cntr++;
    } else {
      HPS_ERR("ft_get_cq_comp %d", cq_ret);
      this->recv_buf->releaseLock();
      return (int) cq_ret;
    }
  }
  this->recv_buf->releaseLock();
  return 0;
}

Connection::~Connection() {

}

int Connection::OnEvent(enum rdma_loop_event event, enum rdma_loop_status state) {
  // HPS_INFO("Connection ready %d", fd);
  if (event == CQ_READ) {
    TransmitComplete();
  } else if (event == CQ_TRANSMIT) {
    ReceiveComplete();
  }
  return 0;
}

int Connection::Disconnect() {
  if (this->ep) {
    int ret = fi_shutdown(ep, 0);
    if (ret) {
      HPS_ERR("Failed to shutdown connection");
    }
    return ret;
  } else {
    HPS_ERR("Not connected");
  }
  return 1;
}

char* Connection::getIPAddress() {
  struct sockaddr_storage addr;
  size_t size;
  int ret;

  ret = fi_getpeer(ep, &addr, &size);
  if (ret) {
    if (ret == -FI_ETOOSMALL) {
      HPS_ERR("FI_ETOOSMALL, we shouln't get this");
    } else {
      HPS_ERR("Failed to get peer address");
    }
  }

  char *addr_str = new char[INET_ADDRSTRLEN];
  struct sockaddr_in* addr_in = (struct sockaddr_in*)(&addr);
  inet_ntop(addr_in->sin_family, &(addr_in->sin_addr), addr_str, INET_ADDRSTRLEN);
  HPS_INFO("Address: %s", addr_str);
  return addr_str;
}

uint32_t Connection::getPort() {
  struct sockaddr_storage addr;
  size_t size;
  int ret;

  ret = fi_getpeer(ep, &addr, &size);
  if (ret) {
    if (ret == -FI_ETOOSMALL) {
      HPS_ERR("FI_ETOOSMALL, we shouln't get this");
    } else {
      HPS_ERR("Failed to get peer address");
    }
  }
  uint32_t port = ntohs(((struct sockaddr_in*)(&addr))->sin_port);
  HPS_INFO("Port: %d", port);
  return port;
}
