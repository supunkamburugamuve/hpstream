
#include "rdma_rdm_channel.h"

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

#include "rdma_connection.h"

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


RDMADatagramChannel::RDMADatagramChannel(RDMAOptions *opts, struct fi_info *info,
                                         struct fid_domain *domain, struct fid_ep *ep,
                                         uint16_t stream_id, uint16_t recv_stream_id,
                                         fi_addr_t	remote_addr) {
  this->options = opts;
  this->info = info;
  this->domain = domain;

  this->ep = ep;
  this->mr = NULL;
  this->w_mr = NULL;

  this->buf = NULL;
  this->w_buf = NULL;
  this->recv_buf = NULL;
  this->send_buf = NULL;

  this->tx_seq = 0;
  this->rx_seq = 0;
  this->tx_cq_cntr = 0;
  this->rx_cq_cntr = 0;

  this->peer_credit = 0;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
  this->stream_id = stream_id;
  this->target_stream_id = recv_stream_id;
  this->remote_addr = remote_addr;
  this->started = false;
}

RDMADatagramChannel::~RDMADatagramChannel() {

}

void RDMADatagramChannel::Free() {
  HPS_CLOSE_FID(mr);
  HPS_CLOSE_FID(w_mr);
  HPS_CLOSE_FID(ep);

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

int RDMADatagramChannel::registerWrite(VCallback<int> onWrite) {
  this->onWriteReady = std::move(onWrite);
  return 0;
}

int RDMADatagramChannel::registerRead(VCallback<int> onRead) {
  this->onReadReady = std::move(onRead);
  return 0;
}

int RDMADatagramChannel::setOnWriteComplete(VCallback<uint32_t> onWriteComplete) {
  this->onWriteComplete = std::move(onWriteComplete);
  return 0;
}

int RDMADatagramChannel::start() {
  int ret = 0;

  if (started) {
    return 0;
  }

  uint16_t message_type = 1;
  uint16_t credit_type = 1;
  send_tag = 0;
  recv_tag = 0;
  // create the receive tag and send tag
  send_tag = (uint64_t)stream_id << 32 | (uint64_t)target_stream_id << 48 | (uint64_t) message_type;
  send_credit_tag = (uint64_t)stream_id << 32 | (uint64_t)target_stream_id << 48 | (uint64_t) message_type | (uint64_t) credit_type << 16;
  recv_tag = (uint64_t)target_stream_id << 32 | (uint64_t)stream_id << 48 | (uint64_t) message_type;

  uint64_t mask = 0;
  for (int i = 0; i < 16; i++) {
    mask = mask | ((uint64_t)1 << i);
  }
  tag_mask = ~mask;
  LOG(INFO) << "Mask of stream id: " << stream_id << " target_id: " << target_stream_id << " mask: " << tag_mask;

  ret = AllocateBuffers();
  if (ret) {
    LOG(ERROR) << "Failed to allocate the buffers: " << ret;
    return ret;
  }
  ret = PostBuffers();
  if (ret) {
    LOG(ERROR) << "Failed to post the buffers: " << ret;
    return ret;
  }
  started = true;
  return 0;
}

int RDMADatagramChannel::PostBuffers() {
  this->rx_seq = 0;
  this->rx_cq_cntr = 0;
  this->tx_cq_cntr = 0;
  this->tx_seq = 0;
  ssize_t ret = 0;
  RDMABuffer *rBuf = this->recv_buf;
  uint32_t noBufs = rBuf->GetNoOfBuffers();
  for (uint32_t i = 0; i < noBufs; i++) {
    ret = PostRX(rBuf->GetBufferSize(), i);
    if (ret) {
      LOG(ERROR) << "Error posting receive buffer" << ret;
      return (int) ret;
    }
    rBuf->IncrementSubmitted(1);
  }

  this->peer_credit = rBuf->GetNoOfBuffers()  - 2;
  this->total_used_credit = 0;
  this->credit_used_checkpoint = 0;
  this->credit_messages_ = new bool[noBufs];
  this->waiting_for_credit = false;
  memset(this->credit_messages_, 0, sizeof(bool) * noBufs);

  return 0;
}

int RDMADatagramChannel::AllocateBuffers(void) {
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
    LOG(INFO) << "register memory with key: " << HPS_MR_KEY + target_stream_id;
    ret = fi_mr_reg(domain, buf, rx_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY + 10 + target_stream_id, 0, &mr, NULL);
    if (ret) {
      LOG(FATAL) << "Failed to register memory: " << ret;
      return ret;
    }
    LOG(INFO) << "register memory with key: " << HPS_MR_KEY_W + target_stream_id;
    ret = fi_mr_reg(domain, w_buf, tx_size, hps_utils_caps_to_mr_access(info->caps),
                    0, HPS_MR_KEY_W + 10 + target_stream_id, 0, &w_mr, NULL);
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

ssize_t RDMADatagramChannel::PostTX(size_t size, int index, uint64_t tag) {
  ssize_t ret;
  struct fi_msg_tagged *msg = &(tag_messages[index]);
  uint8_t *buf = send_buf->GetBuffer(index);
  struct iovec *io = &(io_vectors[index]);

  io->iov_len = size;
  io->iov_base = buf;

  msg->msg_iov = io;
  msg->desc = (void **) fi_mr_desc(mr);
  msg->iov_count = 1;
  msg->addr = remote_addr;
  msg->tag = tag;
  msg->ignore = tag_mask;
  msg->context = &(tx_contexts[index]);

  // LOG(INFO) << "Sending message with tag: " << send_tag << " mask: " << tag_mask;

  ret = fi_tsendmsg(this->ep, (const fi_msg_tagged *) msg, 0);
  if (ret)
    return ret;
  tx_seq++;
  return 0;
}

ssize_t RDMADatagramChannel::PostRX(size_t size, int index) {
  ssize_t ret;
  struct fi_msg_tagged *msg = &(tag_messages[index]);
  uint8_t *buf = recv_buf->GetBuffer(index);
  struct iovec *io = &(io_vectors[index]);

  io->iov_len = size;
  io->iov_base = buf;

  msg->msg_iov = io;
  msg->desc = (void **) fi_mr_desc(mr);
  msg->iov_count = 1;
  msg->addr = remote_addr;
  msg->tag = recv_tag;
  msg->ignore = tag_mask;
  msg->context = &(recv_contexts[index]);

  // LOG(INFO) << "Post receive buffer with tag: " << recv_tag << " mask: " << tag_mask;
  ret = fi_trecvmsg(this->ep, (const fi_msg_tagged *) msg, 0);
  if (ret)
    return ret;
  rx_seq++;
  return 0;
}

bool RDMADatagramChannel::DataAvailableForRead() {
  RDMABuffer *sbuf = this->recv_buf;
  return sbuf->GetFilledBuffers() > 0;
}

int RDMADatagramChannel::ReadData(uint8_t *buf, uint32_t size, uint32_t *read) {
  ssize_t ret = 0;
  uint32_t base;
  uint32_t submittedBuffers;
  uint32_t noOfBuffers;
  uint32_t index = 0;
  // go through the buffers
  RDMABuffer *rbuf = this->recv_buf;
  // now lock the buffer
  if (rbuf->GetFilledBuffers() == 0) {
    *read = 0;
    return 0;
  }
  uint32_t tail = rbuf->GetBase();
  uint32_t buffers_filled = rbuf->GetFilledBuffers();
  uint32_t current_read_indx = rbuf->GetCurrentReadIndex();
  // need to copy
  uint32_t need_copy = 0;
  // number of bytes copied
  uint32_t read_size = 0;
  while (read_size < size &&  buffers_filled > 0) {
    uint8_t *b = rbuf->GetBuffer(tail);
    uint32_t *length;
    // first read the amount of data in the buffer
    length = (uint32_t *) b;
    uint32_t *credit = (uint32_t *) (b + sizeof(uint32_t));
    // update the peer credit with the latest
    if (*credit > 0) {
//      LOG(INFO) << "Incrementing peer credit: " << peer_credit << " by: " << *credit;
      this->peer_credit += *credit;
      if (this->peer_credit > rbuf->GetNoOfBuffers() - 2) {
        this->peer_credit = rbuf->GetNoOfBuffers() - 2;
      }
      if (waiting_for_credit) {
        onWriteReady(0);
      }
//      LOG(INFO) << "Received message with credit: " << *credit << " peer credit: " << peer_credit;
      // lets mark it zero in case we are not moving to next buffer
      *credit = 0;
    } else {
//      LOG(INFO) << "Received message with credit: " << *credit << " peer credit: " << peer_credit;
    }
    // now lets see how much data we need to copy from this buffer
    need_copy = (*length) - current_read_indx;
    // now lets see how much we can copy
    uint32_t can_copy = 0;
    uint32_t tmp_index = current_read_indx;
    // we can copy everything from this buffer
    if (size - read_size >= need_copy) {
      can_copy = need_copy;
      current_read_indx = 0;
      credit_messages_[tail] = length <= 0;
      // advance the base pointer
      rbuf->IncrementBase(1);

      // this->self_credit--;
      buffers_filled--;
      tail = rbuf->GetBase();
    } else {
      // we cannot copy everything from this buffer
      can_copy = size - read_size;
      current_read_indx += can_copy;
    }
    rbuf->setCurrentReadIndex(current_read_indx);
    // next copy the buffer
    memcpy(buf + read_size, b + sizeof(uint32_t) + sizeof(uint32_t) + tmp_index, can_copy);
    // now update
    read_size += can_copy;
  }

  *read = read_size;

  base = rbuf->GetBase();
  submittedBuffers = rbuf->GetSubmittedBuffers();
  noOfBuffers = rbuf->GetNoOfBuffers();
  while (submittedBuffers < noOfBuffers) {
    index = (base + submittedBuffers) % noOfBuffers;
    ret = PostRX(rbuf->GetBufferSize(), index);
    if (ret && ret != -FI_EAGAIN) {
      LOG(ERROR) << "Failed to post the receive buffer: " << ret;
      return (int) ret;
    }
//    LOG(INFO) << "Increment TUC: " << total_used_credit << " by " << 1;
    this->total_used_credit++;
//    LOG(INFO) << "Total used credit: " << total_used_credit << " checkpoint: " << credit_used_checkpoint;
    rbuf->IncrementSubmitted(1);
    submittedBuffers++;
  }

  uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
  if ((available_credit >= (noOfBuffers / 2 - 1))) {
    if (available_credit > noOfBuffers - 2) {
      LOG(ERROR) << "Credit should never be greater than no of buffers available: "
                 << available_credit << " > " << noOfBuffers;
    }
//    LOG(INFO) << "Post credit ReadData: " << total_used_credit << ", " << credit_used_checkpoint;
    postCredit();
  }

  return 0;
}

int RDMADatagramChannel::postCredit() {
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  // now determine the buffer no to use
  uint32_t head = 0;
  uint32_t error_count = 0;
  // we need to send everything by using the buffers available
  uint64_t free_space = sbuf->GetAvailableWriteSpace();
  if (free_space > 0) {
    // we have space in the buffers
    head = sbuf->NextWriteIndex();
    uint8_t *current_buf = sbuf->GetBuffer(head);
    // now lets copy from send buffer to current buffer chosen
    uint32_t *length = (uint32_t *) current_buf;
    // set the first 4 bytes as the content length
    *length = 0;
    // send the credit with the write
    uint32_t *sent_credit = (uint32_t *) (current_buf + sizeof(uint32_t));
    uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
    if (available_credit > sbuf->GetNoOfBuffers() - 1) {
      LOG(ERROR) << "Available credit > no of buffers, something is wrong: "
                 << available_credit << " > " << sbuf->GetNoOfBuffers();
      available_credit = sbuf->GetNoOfBuffers() - 1;
    }
    *sent_credit = available_credit;
//    LOG(INFO) << "Posting credit: " << available_credit << " filled: " << sbuf->GetFilledBuffers();
    // set the data size in the buffer
    sbuf->setBufferContentSize(head, 0);
    // send the current buffer
    ssize_t ret = PostTX(sizeof(uint32_t) + sizeof(int32_t), head, send_credit_tag);
    if (!ret) {
//      LOG(INFO) << "Increment CUC: " << credit_used_checkpoint << " by " << available_credit;
      credit_used_checkpoint += available_credit;
      sbuf->IncrementFilled(1);
      // increment the head
      sbuf->IncrementSubmitted(1);
    } else {
      if (ret != -FI_EAGAIN) {
        LOG(ERROR) << "Failed to transmit the buffer";
        error_count++;
        if (error_count > MAX_ERRORS) {
          LOG(ERROR) << "Failed to send the buffer completely. sent ";
          goto err;
        }
      }
    }
  } else {
//    LOG(ERROR) << "Free space not available to post credit "
//               << " peer: " << peer_credit << "tuc: " << total_used_credit << " cuc: " << credit_used_checkpoint;
  }

  return 0;

  err:
  return 1;
}

int RDMADatagramChannel::WriteData(uint8_t *buf, uint32_t size, uint32_t *write) {
  // first lets get the available buffer
  RDMABuffer *sbuf = this->send_buf;
  // now determine the buffer no to use
  uint32_t sent_size = 0;
  uint32_t current_size = 0;
  uint32_t head = 0;
  uint32_t error_count = 0;
  bool credit_set;
  uint32_t buf_size = sbuf->GetBufferSize() - 8;
  // LOG(INFO) << "Peer credit: " << this->peer_credit;
  // we need to send everything by using the buffers available
  uint32_t free_buffs = sbuf->GetNoOfBuffers() - sbuf->GetFilledBuffers();
  while (sent_size < size && this->peer_credit > 0 && free_buffs > 2) {
    credit_set = false;
    // we have space in the buffers
    head = sbuf->NextWriteIndex();
    uint8_t *current_buf = sbuf->GetBuffer(head);
    // now lets copy from send buffer to current buffer chosen
    current_size = (size - sent_size) < buf_size ? size - sent_size : buf_size;
    uint32_t *length = (uint32_t *) current_buf;
    // set the first 4 bytes as the content length
    *length = current_size;
    // send the credit with the write
    uint32_t *sent_credit = (uint32_t *) (current_buf + sizeof(uint32_t));
    uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
    if (available_credit > 0) {
      if (available_credit > sbuf->GetNoOfBuffers() - 2) {
        LOG(ERROR) << "Available credit > no of buffers, something is wrong: "
                   << available_credit << " > " << sbuf->GetNoOfBuffers();
        available_credit = sbuf->GetNoOfBuffers() - 1;
      }
      *sent_credit =  available_credit;
      credit_set = true;
    } else {
      *sent_credit = 0;
    }
//    LOG(INFO) << "Write credit: " << *sent_credit << " tuc: " << total_used_credit << " cuc: " << credit_used_checkpoint << " peer: " << peer_credit << " filled: " << sbuf->GetFilledBuffers();

    memcpy(current_buf + sizeof(uint32_t) + sizeof(int32_t), buf + sent_size, current_size);
    // set the data size in the buffer
    sbuf->setBufferContentSize(head, current_size);
    // send the current buffer
    ssize_t ret = PostTX(current_size + sizeof(uint32_t) + sizeof(int32_t), head, send_tag);
    if (!ret) {
      if (credit_set) {
//        LOG(INFO) << "Increment CUC: " << credit_used_checkpoint << " by " << available_credit;
        credit_used_checkpoint += available_credit;
      }

      sent_size += current_size;
      sbuf->IncrementFilled(1);
      // increment the head
      sbuf->IncrementSubmitted(1);
      this->peer_credit--;
    } else {
      if (ret != -FI_EAGAIN) {
        LOG(ERROR) << "Failed to transmit the buffer";
        error_count++;
        if (error_count > MAX_ERRORS) {
          LOG(ERROR) << "Failed to send the buffer completely. sent " << sent_size;
          goto err;
        }
      }
    }
    free_buffs = sbuf->GetNoOfBuffers() - sbuf->GetFilledBuffers();
    waiting_for_credit = false;
  }
  if (peer_credit <= 0 && sent_size < size) {
    waiting_for_credit = true;
  }

  *write = sent_size;
  return 0;

  err:
  return -1;
}

int RDMADatagramChannel::ReadReady(ssize_t cq_count){
  if (onReadReady != NULL) {
    this->rx_cq_cntr += cq_count;
    if (recv_buf->GetFilledBuffers() > 0) {
      onReadReady(0);
    }

    if (this->recv_buf->IncrementFilled((uint32_t) cq_count)) {
      LOG(ERROR) << "Failed to increment buffer data pointer";
      return 1;
    }

    if (recv_buf->GetFilledBuffers() > 0) {
      onReadReady(0);
    }
  } else {
     LOG(ERROR) << "Calling read without setting the callback function";
    return -1;
  }
  return 0;
}

int RDMADatagramChannel::WriteReady(ssize_t cq_ret){
  uint32_t completed_bytes = 0;
  if (onWriteReady != NULL) {
    this->tx_cq_cntr += cq_ret;
    uint64_t free_space = send_buf->GetAvailableWriteSpace();
    if (free_space > 0) {
//    LOG(INFO) << "Caling write ready";
      onWriteReady(0);
    }

    for (int i = 0; i < cq_ret; i++) {
      uint32_t base = this->send_buf->GetBase();
      completed_bytes += this->send_buf->getContentSize(base);
      if (this->send_buf->IncrementBase((uint32_t) 1)) {
        LOG(ERROR) << "Failed to increment buffer data pointer";
        return 1;
      }
//      LOG(INFO) << "Write completed: " << send_buf->GetFilledBuffers();
    }

    if (onWriteComplete != NULL && completed_bytes > 0) {
      // call the calback with the completed bytes
      onWriteComplete(completed_bytes);
    }

    free_space = send_buf->GetAvailableWriteSpace();
    if (free_space > 0) {
//    LOG(INFO) << "Caling write ready";
      onWriteReady(0);
    }
  } else {
    LOG(ERROR) << "Calling write without setting the callback function";
    return -1;
  }
  return 0;
}

int RDMADatagramChannel::CreditWriteComplete(){
  this->tx_cq_cntr += 1;
  if (this->send_buf->IncrementBase((uint32_t) 1)) {
    LOG(ERROR) << "Failed to increment buffer data pointer";
    return 1;
  }
  uint32_t noOfBuffers = recv_buf->GetNoOfBuffers();
  uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
//   LOG(INFO) << "Credit write complete: " << available_credit << " write space: " << send_buf->GetFilledBuffers();
  if ((available_credit >= (noOfBuffers / 2 - 1))) {
    if (available_credit > noOfBuffers - 1) {
      LOG(ERROR) << "Credit should never be greater than no of buffers available: "
                 << available_credit << " > " << noOfBuffers;
    }
    postCredit();
  }
  return 0;
}

int RDMADatagramChannel::CreditReadComplete(){
  ssize_t ret = 0;
  uint32_t base;
  uint32_t submittedBuffers;
  uint32_t noOfBuffers;
  uint32_t index = 0;
  // go through the buffers
  RDMABuffer *rbuf = this->recv_buf;

  this->rx_cq_cntr += 1;
  if (this->recv_buf->IncrementFilled((uint32_t) 1)) {
    LOG(ERROR) << "Failed to increment buffer data pointer";
    return 1;
  }

  uint32_t tail = rbuf->GetBase();
  uint32_t buffers_filled = rbuf->GetFilledBuffers();
  // need to copy
  if (buffers_filled > 0) {
    uint8_t *b = rbuf->GetBuffer(tail);
    uint32_t *length;
    // first read the amount of data in the buffer
    length = (uint32_t *) b;

    if (*length != 0) {
      LOG(ERROR) << "Credit message with length != 0";
      return 0;
    }

    uint32_t *credit = (uint32_t *) (b + sizeof(uint32_t));
//    LOG(INFO) << "Credit Received message with credit: " << *credit << " peer credit: " << peer_credit;
    // update the peer credit with the latest
    if (*credit > 0) {
//      LOG(INFO) << "Incrementing peer credit: " << peer_credit << " by: " << *credit;
      this->peer_credit += *credit;
      if (this->peer_credit > rbuf->GetNoOfBuffers() - 2) {
        LOG(WARNING) << "Peer credit greater than number of buffers ********* ";
        this->peer_credit = rbuf->GetNoOfBuffers() - 2;
      }
      if (waiting_for_credit) {
        onWriteReady(0);
      }
    }
    credit_messages_[tail] = length <= 0;
    // advance the base pointer
    rbuf->IncrementBase(1);
  }


  base = rbuf->GetBase();
  submittedBuffers = rbuf->GetSubmittedBuffers();
  noOfBuffers = rbuf->GetNoOfBuffers();
  while (submittedBuffers < noOfBuffers) {
    index = (base + submittedBuffers) % noOfBuffers;
    ret = PostRX(rbuf->GetBufferSize(), index);
    if (ret && ret != -FI_EAGAIN) {
      LOG(ERROR) << "Failed to post the receive buffer: " << ret;
      return (int) ret;
    }
    // this->total_used_credit++;
//    LOG(INFO) << "Total used credit: " << total_used_credit << " checkpoint: " << credit_used_checkpoint;
    rbuf->IncrementSubmitted(1);
    submittedBuffers++;
  }

  uint32_t available_credit = (uint32_t) (total_used_credit - credit_used_checkpoint);
  if ((available_credit >= (noOfBuffers / 2 - 1))) {
    if (available_credit > noOfBuffers - 1) {
      LOG(ERROR) << "Credit should never be greater than no of buffers available: "
                 << available_credit << " > " << noOfBuffers;
    }
//    LOG(INFO) << "Post credit CreditReadComplete" << total_used_credit << "," << credit_used_checkpoint;
    postCredit();
  }
  return 0;
}

int RDMADatagramChannel::ConnectionClosed() {
  Free();
  return 0;
}

int RDMADatagramChannel::closeConnection() {
  Free();
  return 0;
}

char* RDMADatagramChannel::getIPAddress() {
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

uint32_t RDMADatagramChannel::getPort() {
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
