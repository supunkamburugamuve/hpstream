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

static void *startRDMLoopThread(void *param) {
  RDMADatagram *loop = static_cast<RDMADatagram *>(param);
  loop->Loop();
  return NULL;
}

RDMADatagram::RDMADatagram(RDMAOptions *opts, RDMAFabric *fabric, uint32_t stream_id) {
  this->options = opts;
  this->info = fabric->GetInfo();
  this->info_hints = fabric->GetHints();
  this->fabric = fabric->GetFabric();

  LOG(INFO) << "RDMA Datagram info";
  print_info(this->info);

  this->txcq = NULL;
  this->rxcq = NULL;
  this->av = NULL;
  this->av_attr.type = FI_AV_MAP;
  this->av_attr.count = 1;

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

  this->tx_seq = 0;
  this->rx_seq = 0;
  this->tx_cq_cntr = 0;
  this->rx_cq_cntr = 0;
  this->onRDMConnect = NULL;
  this->onRDMConfirm = NULL;

  this->cq_attr.wait_obj = FI_WAIT_NONE;
  this->stream_id = stream_id;
  this->run = true;
}

void RDMADatagram::Free() {
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

int RDMADatagram::start() {
  LOG(INFO) << "Starting rdma loo[  ";
  int ret;

  ret = fi_domain(this->fabric, this->info, &this->domain, NULL);
  if (ret) {
    LOG(ERROR) << "fi_domain " << ret;
    return ret;
  }

  ret = InitEndPoint();
  if (ret) {
    LOG(ERROR) << "Failed to initialize endpoint";
    return ret;
  }

  ret = SetupQueues();
  if (ret) {
    LOG(ERROR) << "Failed to setup queues";
    return ret;
  }
  LOG(INFO) << "Queues setup";

  ret = PostBuffers();
  if (ret) {
    LOG(ERROR) << "Failed to allocate the buffers";
  }
  LOG(INFO) << "Posted buffers";

  uint64_t mask = 0;
  for (int i = 0; i < 16; i++) {
    mask = mask | ((uint64_t)1 << i);
  }
  tag_mask = ~mask;
  recv_tag = (uint64_t) 0;

  //start the loop thread
  ret = pthread_create(&loopThreadId, NULL, &startRDMLoopThread, (void *) this);
  if (ret) {
    LOG(ERROR) << "Failed to create thread " << ret;
    return ret;
  }

  return 0;
}

int RDMADatagram::PostBuffers() {
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
    ret = PostRX(rBuf->GetBufferSize(), i);
    if (ret) {
      LOG(ERROR) << "Error posting receive buffer" << ret;
      return (int) ret;
    }
    rBuf->IncrementSubmitted(1);
  }

  return 0;
}

void RDMADatagram::Loop() {
  while (run) {
    TransmitComplete();
    ReceiveComplete();
  }
}

RDMADatagramChannel* RDMADatagram::CreateChannel(uint32_t target_id, struct fi_info *target) {
  fi_addr_t remote_addr;
  int ret;
  RDMADatagramChannel *channel;
  channel = GetChannel(target_id);
  if (channel == NULL) {
    ret = AVInsert(target->dest_addr, 1, &remote_addr, 0, NULL);
    if (ret) {
      LOG(ERROR) << "Failed to get target address information: " << ret;
      return NULL;
    }

    channel = new RDMADatagramChannel(options, info, domain, stream_id, target_id, remote_addr);
    channels[target_id] = channel;
  }
  return channel;
}

RDMADatagramChannel* RDMADatagram::GetChannel(uint32_t target_id) {
  std::unordered_map<std::uint32_t, RDMADatagramChannel*>::const_iterator it = channels.find(target_id);
  if (it == channels.end()) {
    return NULL;
  } else {
    return it->second;
  }
}

int RDMADatagram::SetupQueues() {
  int ret;
  ret = AllocateBuffers();
  if (ret) {
    LOG(ERROR) << "Buffer allocation failed";
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
  cq_attr.wait_obj = FI_WAIT_UNSPEC;
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

int RDMADatagram::AllocateBuffers(void) {
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
    LOG(INFO) << "Register memory using key: " << HPS_MR_KEY;
    ret = fi_mr_reg(domain, buf, rx_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY, 0, &mr, NULL);
    if (ret) {
      LOG(FATAL) << "Failed to register memory: " << ret;
      return ret;
    }
    LOG(INFO) << "Register memory using key: " << HPS_MR_KEY_W;
    ret = fi_mr_reg(domain, w_buf, tx_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY_W , 0, &w_mr, NULL);
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
  this->io_vectors = new struct iovec[opts->no_buffers];
  this->tag_messages = new struct fi_msg_tagged[opts->no_buffers];
  this->recv_contexts = new struct fi_context[opts->no_buffers];
  this->tx_contexts = new struct fi_context[opts->no_buffers];
  return 0;
}

int RDMADatagram::InitEndPoint() {
  int ret;

  // create the end point for this connection
  ret = fi_endpoint(domain, this->info, &ep, NULL);
  if (ret) {
    LOG(ERROR) << "fi_endpoint" << ret;
    return ret;
  }

  HPS_EP_BIND(ep, av, 0);
  HPS_EP_BIND(ep, txcq, FI_TRANSMIT);
  HPS_EP_BIND(ep, rxcq, FI_RECV);

  ret = hps_utils_get_cq_fd(this->options, txcq, &tx_fd);
  if (ret) {
    LOG(ERROR) << "Failed to get cq fd for transmission";
    return ret;
  }

  ret = hps_utils_get_cq_fd(this->options, rxcq, &rx_fd);
  if (ret) {
    LOG(ERROR) << "Failed to get cq fd for receive";
    return ret;
  }

  ret = fi_enable(ep);
  if (ret) {
    LOG(ERROR) << "Failed to enable endpoint " << ret;
    return ret;
  }
  return 0;
}

int RDMADatagram::AVInsert(void *addr, size_t count, fi_addr_t *fi_addr,
                 uint64_t flags, void *context) {
  int ret;

  ret = fi_av_insert(av, addr, count, fi_addr, flags, context);
  if (ret < 0) {
    LOG(ERROR) << "fi_av_insert " << ret;
    return ret;
  } else if (ret != count) {
    LOG(ERROR) << "fi_av_insert: number of addresses inserted = %d;"
               " number of addresses given: " << count << "," << ret;
    return -EXIT_FAILURE;
  }

  return 0;
}

ssize_t RDMADatagram::PostTX(size_t size, int index, fi_addr_t addr, uint32_t send_id, uint16_t type) {
  ssize_t ret;
  uint64_t send_tag = 0;
  send_tag |= (uint64_t)type << 16 | (uint64_t)send_id << 32;
  struct fi_msg_tagged *msg = &(tag_messages[index]);
  uint8_t *buf = recv_buf->GetBuffer(index);
  struct iovec *io = &(io_vectors[index]);

  io->iov_len = size;
  io->iov_base = buf;

  msg->msg_iov = io;
  msg->desc = (void **) fi_mr_desc(mr);
  msg->iov_count = 1;
  msg->addr = addr;
  msg->tag = send_tag;
  msg->ignore = tag_mask;
  msg->context = &(recv_contexts[index]);

  ret = fi_tsendmsg(this->ep, (const fi_msg_tagged *) msg, 0);
  if (ret)
    return ret;
  rx_seq++;
  return 0;
}

ssize_t RDMADatagram::PostRX(size_t size, int index) {
  ssize_t ret;
  struct fi_msg_tagged *msg = &(tag_messages[index]);
  uint8_t *buf = recv_buf->GetBuffer(index);
  struct iovec *io = &(io_vectors[index]);

  io->iov_len = size;
  io->iov_base = buf;

  msg->msg_iov = io;
  msg->desc = (void **) fi_mr_desc(mr);
  msg->iov_count = 1;
  msg->addr = FI_ADDR_UNSPEC;
  msg->tag = recv_tag;
  msg->ignore = tag_mask;
  msg->context = &(tx_contexts[index]);

  if (ep->tagged == NULL) {
    LOG(ERROR) << "No tagged messaging";
  }

  ret = fi_trecvmsg(this->ep, (const fi_msg_tagged *) msg, 0);
  if (ret)
    return ret;
  rx_seq++;
  return 0;
}

int RDMADatagram::SendAddressToRemote(uint32_t remote) {
  size_t addrlen;
  ssize_t ret;
  addrlen = send_buf->GetBufferSize();
  uint32_t head = 0;
  head = send_buf->NextWriteIndex();
  uint8_t *send_buffer = send_buf->GetBuffer(head);
  ret = fi_getname(&ep->fid, (char *) send_buffer, &addrlen);
  if (ret) {
    LOG(ERROR) << "Failed to get network name";
    return (int) ret;
  }
  RDMADatagramChannel *channel = GetChannel(remote);
  ret = PostTX(addrlen, head, channel->GetRemoteAddress(), stream_id, 0);
  if (ret) {
    LOG(ERROR) << "Failed to send the address to remote";
    return (int) ret;
  }
  return 0;
}

int RDMADatagram::SendConfirmToRemote(fi_addr_t remote) {
  ssize_t ret;
  uint32_t head = 0;
  head = send_buf->NextWriteIndex();

  ret = PostTX(1, head, remote, stream_id, 0);
  if (ret) {
    LOG(ERROR) << "Failed to send the address to remote";
    return (int) ret;
  }
  return 0;
}

int RDMADatagram::HandleConnect(uint16_t connect_type, int bufer_index, uint32_t target_id) {
  int ret;
  // server receive the connection information
  if (connect_type == 0) {
    fi_addr_t remote_addr;
    uint8_t *buf = recv_buf->GetBuffer(bufer_index);
    ret = AVInsert(buf, 1, &remote_addr, 0, NULL);
    if (ret) {
      LOG(ERROR) << "Failed to get target address information: " << ret;
      return NULL;
    }
    ret = AVInsert(buf, 1, &remote_addr, 0, NULL);
    if (ret) {
      LOG(ERROR) << "Failed to get target address information: " << ret;
      return NULL;
    }
    RDMADatagramChannel *channel = new RDMADatagramChannel(options, info, domain, stream_id, target_id, remote_addr);
    channels[target_id] = channel;

    if (onRDMConnect) {
      onRDMConnect(target_id);
    } else {
      LOG(ERROR) << "Received connect but callback is not set";
      return -1;
    }
    this->recv_buf->IncrementBase(1);
    ret = SendConfirmToRemote(remote_addr);
    if (ret) {
      LOG(ERROR) << "Failed to send confirmation";
      return -1;
    }
  } else if (connect_type == 1) {
    uint32_t *id = (uint32_t *) (buf);
    if (onRDMConfirm) {
      onRDMConfirm(*id);
    } else {
      LOG(ERROR) << "Received connect confirm but callback is not set";
      return -1;
    }
  }

  return 0;
}

int RDMADatagram::TransmitComplete() {
  struct fi_cq_tagged_entry comp;
  ssize_t cq_ret;
  RDMABuffer *sbuf = this->send_buf;
  // lets get the number of completions
  size_t max_completions = tx_seq - tx_cq_cntr;
  size_t completions_count = 0;

  while (completions_count < max_completions) {
    cq_ret = fi_cq_read(txcq, &comp, 1);

    if (cq_ret == 0 || cq_ret == -FI_EAGAIN) {
      return 0;
    }

    if (cq_ret > 0) {
      // extract the type of message
      uint16_t type = (uint16_t) comp.tag;
      uint32_t stream_id = ((uint32_t) (comp.tag >> 32));
      if (type == 0) {       // control message
        this->tx_cq_cntr += cq_ret;
        for (int i = 0; i < cq_ret; i++) {
          if (this->send_buf->IncrementBase((uint32_t) cq_ret)) {
            LOG(ERROR) << "Failed to increment buffer data pointer";
            return 1;
          }
        }
        uint16_t control_type = (uint16_t) (comp.tag >> 16);
        // initial contact
        if (control_type == 0) {
          LOG(INFO) << "Sent the connect message";
        }
      } else if (type == 1) {  // data message
        // pick te correct channel
        std::unordered_map<uint32_t, RDMADatagramChannel *>::const_iterator it
            = channels.find(stream_id);
        if (it == channels.end()) {
          LOG(ERROR) << "Un-expected stream id in tag: " << stream_id;
          return -1;
        } else {
          RDMADatagramChannel *channel = it->second;
          if (channel->WriteReady(cq_ret)) {
            LOG(ERROR) << "Failed to read";
            return -1;
          }
        }
      }

    } else if (cq_ret < 0) {
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

    completions_count++;
  }

  // go through the channels and figure out the number of expected completions
  for (auto it = channels.begin(); it != channels.end(); ++it) {
    RDMADatagramChannel *channel = it->second;
    // we call ready in case we haven't read all the data from the buffers
    channel->WriteReady(0);
    tx_seq += channel->WritePostCount();
    tx_cq_cntr += channel->WriteCompleteCount();
  }

  return 0;
}

int RDMADatagram::ReceiveComplete() {
  ssize_t cq_ret;
  struct fi_cq_tagged_entry comp;
  RDMABuffer *recvBuf = this->recv_buf;
  // lets get the number of completions
  size_t max_completions = rx_seq - rx_cq_cntr;
  uint64_t read_available = recvBuf->GetFilledBuffers();
  size_t current_count = 0;
  while (current_count < max_completions) {
    // we can expect up to this
    cq_ret = fi_cq_read(rxcq, &comp, 1);
    if (cq_ret == 0 || cq_ret == -FI_EAGAIN) {
      break;
    }
    LOG(INFO) << "Receive complete " << cq_ret;

    if (cq_ret > 0) {
      // extract the type of message
      uint16_t type = (uint16_t) comp.tag;
      uint32_t stream_id = ((uint32_t) (comp.tag >> 32));
      if (type == 0) {       // control message
        this->rx_cq_cntr += cq_ret;
        if (this->recv_buf->IncrementFilled((uint32_t) cq_ret)) {
          LOG(ERROR) << "Failed to increment buffer data pointer";
          return 1;
        }

        uint16_t control_type = (uint16_t) (comp.tag >> 16);
        // initial contact
        uint32_t tail = recvBuf->GetBase();
        HandleConnect(control_type, tail, stream_id);
      } else if (type == 1) {  // data message
        // pick te correct channel
        std::unordered_map<uint32_t, RDMADatagramChannel *>::const_iterator it
            = channels.find(stream_id);
        if (it == channels.end()) {
          LOG(ERROR) << "Un-expected stream id in tag: " << stream_id;
          return -1;
        } else {
          RDMADatagramChannel *channel = it->second;
          if (channel->ReadReady(cq_ret)) {
            LOG(ERROR) << "Failed to read";
            return -1;
          }
        }
      }
    } else if (cq_ret < 0) {
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
    current_count++;
  }

  // go through the channels and figure out the number of expected completions
  for (auto it = channels.begin(); it != channels.end(); ++it) {
    RDMADatagramChannel *channel = it->second;
    // we call ready in case we haven't read all the data from the buffers
    channel->ReadReady(0);
    rx_seq += channel->ReadPostCount();
    rx_cq_cntr += channel->ReadCompleteCount();
  }

  return 0;
}

RDMADatagram::~RDMADatagram() {}

void RDMADatagram::OnWrite(enum rdma_loop_status state) {
  TransmitComplete();
}

void RDMADatagram::OnRead(enum rdma_loop_status state) {
  ReceiveComplete();
}

int RDMADatagram::ConnectionClosed() {
  Free();
  return 0;
}

int RDMADatagram::closeConnection() {
  Free();
  return 0;
}

int RDMADatagram::Wait() {
  pthread_join(loopThreadId, NULL);
  return 0;
}

int RDMADatagram::Stop() {
  this->run = false;
  return 0;
}