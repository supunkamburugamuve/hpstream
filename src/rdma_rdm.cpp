#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <iostream>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <arpa/inet.h>
#include <glog/logging.h>

#include "rdma_rdm.h"

#define HPS_EP_BIND(ep, fd, flags)					\
	do {								\
		int ret;						\
		if ((fd)) {						\
			ret = fi_ep_bind((ep), &(fd)->fid, (flags));	\
			if (ret) {					\
				LOG(ERROR) << "fi_ep_bind " << ret;		\
				return ret;				\
			}						\
		}							\
	} while (0)


Datagram::Datagram(RDMAOptions *opts, struct fi_info *info,
                                       struct fid_fabric *fabric, struct fid_domain *domain,
                                       RDMAEventLoop *loop) {
  this->options = opts;
  this->info = info;
  this->info_hints = info_hints;
  this->fabric = fabric;
  this->domain = domain;
  this->eventLoop = loop;

  this->txcq = NULL;
  this->rxcq = NULL;
  this->av = NULL;

  this->tx_loop.callback = [this](enum rdma_loop_status state) { return this->OnWrite(state); };
  this->rx_loop.callback = [this](enum rdma_loop_status state) { return this->OnRead(state); };
  this->tx_loop.valid = true;
  this->rx_loop.valid = true;
  this->tx_loop.event = CQ_TRANSMIT;
  this->rx_loop.event = CQ_READ;

  this->ep = NULL;
  this->alias_ep = NULL;
  this->mr = NULL;
  this->w_mr = NULL;

  this->rx_fd = 0;
  this->tx_fd = 0;

  this->buf = NULL;
  this->w_buf = NULL;
  this->recv_buf = NULL;
  this->send_buf = NULL;

  this->cq_attr = {};
  this->tx_ctx = {};
  this->rx_ctx = {};

  this->tx_seq = 0;
  this->rx_seq = 0;
  this->tx_cq_cntr = 0;
  this->rx_cq_cntr = 0;

  this->cq_attr.wait_obj = FI_WAIT_NONE;

  this->self_credit = 0;
  this->total_sent_credit = 0;
  this->peer_credit = 0;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
}

void Datagram::Free() {
  HPS_CLOSE_FID(mr);
  HPS_CLOSE_FID(w_mr);
  HPS_CLOSE_FID(alias_ep);
  HPS_CLOSE_FID(ep);
  HPS_CLOSE_FID(rxcq);
  HPS_CLOSE_FID(txcq);

  if (buf) {
    free(buf);
  }

  if (w_buf) {
    free(w_buf);
  }

  if (recv_buf) {
    recv_buf->Free();
    delete recv_buf;
  }

  if (send_buf) {
    send_buf->Free();
    delete send_buf;
  }
}

int Datagram::start() {
  LOG(INFO) << "Starting rdma connection";
  int ret = PostBuffers();
  if (ret) {
    LOG(ERROR) << "Failed to set up the buffers " << ret;
    return ret;
  }

  // registe with the loop
  ret = this->eventLoop->RegisterRead(&rx_loop);
  if (ret) {
    LOG(ERROR) << "Failed to register receive cq to event loop " << ret;
    return ret;
  }

  ret = this->eventLoop->RegisterRead(&tx_loop);
  if (ret) {
    LOG(ERROR) << "Failed to register transmit cq to event loop " << ret;
    return ret;
  }

  return 0;
}

int Datagram::registerWrite(VCallback<int> onWrite) {
  this->onWriteReady = std::move(onWrite);
  return 0;
}

int Datagram::registerRead(VCallback<int> onRead) {
  this->onReadReady = std::move(onRead);
  return 0;
}

int Datagram::setOnWriteComplete(VCallback<uint32_t> onWriteComplete) {
  this->onWriteComplete = std::move(onWriteComplete);
  return 0;
}

int Datagram::SetupQueues() {
  int ret;
  ret = AllocateBuffers();
  if (ret) {
    return ret;
  }

  // we use the context, not the counter
  cq_attr.format = FI_CQ_FORMAT_TAGGED;
  // create a file descriptor wait cq set
  cq_attr.wait_obj = FI_WAIT_UNSPEC;
  cq_attr.wait_cond = FI_CQ_COND_NONE;
  cq_attr.size = send_buf->GetNoOfBuffers();
  ret = fi_cq_open(domain, &cq_attr, &txcq, &txcq);
  if (ret) {
    LOG(ERROR) << "fi_cq_open for send " << ret;
    return ret;
  }

  // create a file descriptor wait cq set
  cq_attr.wait_obj = FI_WAIT_FD;
  cq_attr.wait_cond = FI_CQ_COND_NONE;
  LOG(INFO) << "RQ Attr size: " << info->rx_attr->size;
  cq_attr.size = send_buf->GetNoOfBuffers();
  ret = fi_cq_open(domain, &cq_attr, &rxcq, &rxcq);
  if (ret) {
    LOG(ERROR) << "fi_cq_open for receive " << ret;
    return ret;
  }

  if (info->ep_attr->type == FI_EP_RDM || info->ep_attr->type == FI_EP_DGRAM) {
    if (info->domain_attr->av_type != FI_AV_UNSPEC)
      av_attr.type = info->domain_attr->av_type;

    ret = fi_av_open(domain, &av_attr, &av, NULL);
    if (ret) {
      LOG(ERROR) << "fi_av_open: " << ret;
      return ret;
    }
  }

  return 0;
}

int Datagram::AllocateBuffers(void) {
  int ret = 0;
  RDMAOptions *opts = this->options;
  uint8_t *tx_buf, *rx_buf;
  size_t tx_size, rx_size;

  tx_size = opts->buf_size;
  rx_size = opts->buf_size;
  if (tx_size > info->ep_attr->max_msg_size) {
    LOG(WARNING) << "Buffer size is greater than max message size, adjusting";
    tx_size = info->ep_attr->max_msg_size;
    rx_size = info->ep_attr->max_msg_size;
  }

  buf = (uint8_t *)malloc(rx_size);
  if (!buf) {
    LOG(FATAL) << "No memory in the system";
    return -FI_ENOMEM;
  }
  w_buf = (uint8_t *)malloc(tx_size);
  if (!w_buf) {
    LOG(FATAL) << "No memory in the system";
    return -FI_ENOMEM;
  }

  memset(buf, 0, rx_size);
  memset(w_buf, 0, tx_size);
  rx_buf = buf;
  tx_buf = w_buf;

  if (((info->mode & FI_LOCAL_MR) ||
       (info->caps & (FI_RMA | FI_ATOMIC)))) {
    ret = fi_mr_reg(domain, buf, rx_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY, 0, &mr, NULL);
    if (ret) {
      LOG(FATAL) << "Failed to register memory: " << ret;
      return ret;
    }
    ret = fi_mr_reg(domain, w_buf, tx_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY_W, 0, &w_mr, NULL);
    if (ret) {
      LOG(FATAL) << "Failed to register memory: " << ret;
      return ret;
    }
  } else {
    LOG(FATAL) << "Failed to register memory due to un-supported capabilities of the provider";
    return 1;
  }

  this->send_buf = new RDMABuffer(tx_buf, (uint32_t) tx_size, opts->no_buffers);
  this->recv_buf = new RDMABuffer(rx_buf, (uint32_t) rx_size, opts->no_buffers);
  return 0;
}

int Datagram::InitEndPoint(struct fid_ep *ep, struct fid_eq *eq) {
  int ret;
  this->ep = ep;

  // only bind the connection oriented endpoints to eq
  if (info->ep_attr->type == FI_EP_MSG) {
    HPS_EP_BIND(ep, eq, 0);
  }
  HPS_EP_BIND(ep, av, 0);
  HPS_EP_BIND(ep, txcq, FI_TRANSMIT);
  HPS_EP_BIND(ep, rxcq, FI_RECV);

  ret = hps_utils_get_cq_fd(this->options, txcq, &tx_fd);
  if (ret) {
    LOG(ERROR) << "Failed to get cq fd for transmission";
    return ret;
  }
  this->tx_loop.fid = tx_fd;
  this->tx_loop.desc = &this->txcq->fid;

  ret = hps_utils_get_cq_fd(this->options, rxcq, &rx_fd);
  if (ret) {
    LOG(ERROR) << "Failed to get cq fd for receive";
    return ret;
  }
  this->rx_loop.fid = rx_fd;
  this->rx_loop.desc = &this->rxcq->fid;

  ret = fi_enable(ep);
  if (ret) {
    LOG(ERROR) << "Failed to enable endpoint " << ret;
    return ret;
  }
  return 0;
}

int Datagram::PostBuffers() {
  this->rx_seq = 0;
  this->rx_cq_cntr = 0;
  this->tx_cq_cntr = 0;
  this->tx_seq = 0;
  ssize_t ret = 0;
  RDMABuffer *rBuf = this->recv_buf;
  uint32_t noBufs = rBuf->GetNoOfBuffers();
  for (uint32_t i = 0; i < noBufs; i++) {
    uint8_t *buf = rBuf->GetBuffer(i);
    // LOG(INFO) << "Posting receive buffer of size: " << rBuf->GetBufferSize();
    ret = PostRX(rBuf->GetBufferSize(), buf, &rx_ctx);
    if (ret) {
      LOG(ERROR) << "Error posting receive buffer" << ret;
      return (int) ret;
    }
    rBuf->IncrementSubmitted(1);
  }
  this->self_credit = rBuf->GetNoOfBuffers();
  this->peer_credit = rBuf->GetNoOfBuffers()  - 1;
  this->total_sent_credit = rBuf->GetNoOfBuffers() - 1;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
  this->credit_messages_ = new bool[noBufs];
  this->waiting_for_credit = false;
  memset(this->credit_messages_, 0, sizeof(bool) * noBufs);
  return 0;
}

ssize_t Datagram::PostTX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  ssize_t ret = 0;

  ret = fi_send(this->ep, buf, size, fi_mr_desc(w_mr),	0, ctx);
  if (ret)
    return ret;

  tx_seq++;
  return 0;
}

ssize_t Datagram::PostRX(size_t size, uint8_t *buf, struct fi_context* ctx) {
  ssize_t ret;

  ret = fi_recv(this->ep, buf, size, fi_mr_desc(mr),	0, ctx);
  if (ret)
    return ret;
  rx_seq++;
  return 0;
}

int Datagram::AvInsert(void *addr, size_t count, fi_addr_t *fi_addr,
                                 uint64_t flags, void *context) {
  int ret;

  ret = fi_av_insert(this->av, addr, count, fi_addr, flags, context);
  if (ret < 0) {
    LOG(ERROR) << "Address vector insert failed: " << ret;
    return ret;
  } else if (ret != count) {
    LOG(ERROR) << "fi_av_insert: number of addresses inserted = %d;" << ret <<
    " number of addresses given = %zd\n" << count;
    return -EXIT_FAILURE;
  }
  return 0;
}

int Datagram::TransmitComplete() {
  struct fi_cq_err_entry comp;
  ssize_t cq_ret;
  uint32_t completed_bytes = 0;
  RDMABuffer *sbuf = this->send_buf;
  // lets get the number of completions
  size_t max_completions = tx_seq - tx_cq_cntr;
  // we can expect up to this
  //LOG(INFO) << "Transmit complete begin";
  uint64_t free_space = sbuf->GetAvailableWriteSpace();

  if (free_space > 0) {
//    LOG(INFO) << "Caling write ready";
    onWriteReady(0);
  }

  cq_ret = fi_cq_read(txcq, &comp, max_completions);
  if (cq_ret == 0 || cq_ret == -FI_EAGAIN) {
//    LOG(INFO) << "transmit complete: " << cq_ret << " expected: " << max_completions
//    << "tx: " << tx_seq << " count: " << tx_cq_cntr;
    return 0;
  }

//  LOG(INFO) << "transmit complete " << cq_ret;
  // LOG(INFO) << "Lock";
  if (cq_ret > 0) {
    this->tx_cq_cntr += cq_ret;
    for (int i = 0; i < cq_ret; i++) {
      uint32_t base = this->send_buf->GetBase();
      completed_bytes += this->send_buf->getContentSize(base);
      if (this->send_buf->IncrementBase((uint32_t) 1)) {
        LOG(ERROR) << "Failed to increment buffer data pointer";
        return 1;
      }
    }
  } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
    // okay we have an error
    if (cq_ret == -FI_EAVAIL) {
      LOG(ERROR) << "Error receive " << cq_ret;
      cq_ret = hps_utils_cq_readerr(txcq);
      this->tx_cq_cntr++;
    } else {
      LOG(ERROR) << "Write completion queue error " << cq_ret;
      return (int) cq_ret;
    }
  }
  if (onWriteComplete != NULL && completed_bytes > 0) {
    // call the calback with the completed bytes
    onWriteComplete(completed_bytes);
  }

  free_space = sbuf->GetAvailableWriteSpace();
  if (free_space > 0) {
//    LOG(INFO) << "Caling write ready";
    onWriteReady(0);
  }

  return 0;
}

int Datagram::ReceiveComplete() {
  ssize_t cq_ret;
  struct fi_cq_tagged_entry comp;
  RDMABuffer *sbuf = this->recv_buf;
  // lets get the number of completions
  size_t max_completions = rx_seq - rx_cq_cntr;
  uint64_t read_available = sbuf->GetFilledBuffers();
  size_t current_count = 0;
  while (current_count < max_completions) {
    // we can expect up to this
    cq_ret = fi_cq_read(rxcq, &comp, 1);
//  LOG(INFO) << "CQ Read " << cq_ret << " expected: " << max_completions << " rx_seq " << rx_seq << " rx_cq_cntr " << rx_cq_cntr;
    if (cq_ret == 0 || cq_ret == -FI_EAGAIN) {
      if (read_available > 0) {
        onReadReady(0);
      }
      return 0;
    }

    if (cq_ret > 0) {
      this->rx_cq_cntr += cq_ret;
      if (this->recv_buf->IncrementFilled((uint32_t) cq_ret)) {
        LOG(ERROR) << "Failed to increment buffer data pointer";
        return 1;
      }
    } else if (cq_ret < 0 && cq_ret != -FI_EAGAIN) {
      // okay we have an error
      if (cq_ret == -FI_EAVAIL) {
        LOG(INFO) << "Error in receive completion" << cq_ret;
        cq_ret = hps_utils_cq_readerr(rxcq);
        this->rx_cq_cntr++;
      } else {
        LOG(ERROR) << "Receive completion queue error" << cq_ret;
        return (int) cq_ret;
      }
    }

    read_available = sbuf->GetFilledBuffers();
    if (read_available > 0) {
      onReadReady(0);
    }
    current_count++;
  }
  return 0;
}

Datagram::~Datagram() {}

void Datagram::OnWrite(enum rdma_loop_status state) {
  TransmitComplete();
}

void Datagram::OnRead(enum rdma_loop_status state) {
  ReceiveComplete();
}

int Datagram::ConnectionClosed() {
  if (eventLoop->UnRegister(&rx_loop)) {
    LOG(ERROR) << "Failed to un-register read from loop";
  }

  if (eventLoop->UnRegister(&tx_loop)) {
    LOG(ERROR) << "Failed to un-register transmit from loop";
  }

  Free();
  return 0;
}

int Datagram::closeConnection() {
  if (eventLoop->UnRegister(&rx_loop)) {
    LOG(ERROR) << "Failed to un-register read from loop";
  }

  if (eventLoop->UnRegister(&tx_loop)) {
    LOG(ERROR) << "Failed to un-register transmit from loop";
  }

  int ret = fi_shutdown(ep, 0);
  if (ret) {
    LOG(ERROR) << "Failed to shutdown connection";
  }

  Free();
  return 0;
}

char* Datagram::getIPAddress() {
  struct sockaddr_storage addr;
  size_t size;
  int ret;

  ret = fi_getpeer(ep, &addr, &size);
  if (ret) {
    if (ret == -FI_ETOOSMALL) {
      LOG(ERROR) << "FI_ETOOSMALL, we shouln't get this";
    } else {
      LOG(ERROR) << "Failed to get peer address";
    }
  }

  char *addr_str = new char[INET_ADDRSTRLEN];
  struct sockaddr_in* addr_in = (struct sockaddr_in*)(&addr);
  inet_ntop(addr_in->sin_family, &(addr_in->sin_addr), addr_str, INET_ADDRSTRLEN);
  return addr_str;
}

uint32_t Datagram::getPort() {
  struct sockaddr_storage addr;
  size_t size;
  int ret;

  ret = fi_getpeer(ep, &addr, &size);
  if (ret) {
    if (ret == -FI_ETOOSMALL) {
      LOG(ERROR) << "FI_ETOOSMALL, we shouln't get this";
    } else {
      LOG(ERROR) << "Failed to get peer address";
    }
  }
  uint32_t port = ntohs(((struct sockaddr_in*)(&addr))->sin_port);
  return port;
}