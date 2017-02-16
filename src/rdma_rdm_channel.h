#ifndef RDMA_RDM_CONNECTION_H_
#define RDMA_RDM_CONNECTION_H_

#include <cstdint>
#include <unistd.h>
#include <stdio.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>
#include <glog/logging.h>

#include "rdma_buffer.h"
#include "utils.h"
#include "options.h"
#include "rdma_event_loop.h"
#include "rdma_channel.h"

class RDMADatagramChannel : public RDMAChannel {
public:

  RDMADatagramChannel(RDMAOptions *opts, struct fi_info *info,
                      struct fid_domain *domain, struct fid_ep *ep,
                      uint16_t stream_id, uint16_t recv_stream_id,
                      fi_addr_t	remote_addr);
  void Free();

  virtual ~RDMADatagramChannel();

  int start();

  int WriteData(uint8_t *buf, uint32_t size, uint32_t *writem);

  int ReadData(uint8_t *buf, uint32_t size, uint32_t *read);

  bool DataAvailableForRead();

  struct fid_ep *GetEp() {
    return ep;
  }

  // disconnect
  int closeConnection();

  // remove end has done a connection close
  int ConnectionClosed();

  uint32_t getPort();

  char *getIPAddress();

  fi_addr_t GetRemoteAddress() {
    return remote_addr;
  }

  int setOnWriteComplete(VCallback<uint32_t> onWriteComplete);

  int registerRead(VCallback<int> onWrite);
  int registerWrite(VCallback<int> onWrite);
  int WriteReady(ssize_t cq_write);
  int ReadReady(ssize_t cq_read);

  uint64_t ReadPostCount() { return rx_seq; }
  uint64_t ReadCompleteCount() { return rx_cq_cntr; };
  uint64_t WritePostCount() { return tx_seq; }
  uint64_t WriteCompleteCount() { return tx_cq_cntr; };
private:
  // options for initialization
  RDMAOptions *options;
  uint16_t stream_id;
  uint16_t target_stream_id;
  // fabric information obtained
  struct fi_info *info;
  // the fabric
  // fabric domain we are working with
  struct fid_domain *domain;
  // end point
  struct fid_ep *ep;
  // cq attribute for getting completion notifications
  struct rdma_loop_info tx_loop, rx_loop;
  // buffer used for communication
  uint8_t *buf;
  uint8_t *w_buf;
  RDMABuffer *recv_buf;
  RDMABuffer *send_buf;

  struct fid_mr *mr;
  struct fid_mr *w_mr;

  // sequence numbers for messages posted
  uint64_t tx_seq;
  // completed transfer requests
  uint64_t tx_cq_cntr;
  // receives posted
  uint64_t rx_seq;
  // receive completed
  uint64_t rx_cq_cntr;

  /**
   * Transmit a buffer
   */
  ssize_t PostTX(size_t size, int index);
  /**
   * Post a receive buffer
   */
  ssize_t PostRX(size_t size, int index);

  /**
   * Allocate the buffers for this communication
   */
  int AllocateBuffers(void);
  int PostBuffers();

  VCallback<uint32_t> onWriteComplete;
  VCallback<int> onWriteReady;
  VCallback<int> onReadReady;

  // credits for the flow control of messages
  // credit for this side, we have posted this many buffers
  int32_t self_credit;
  // this is the last sent credit to the peer
  int32_t total_sent_credit;
  // number of messages received after last sent credit
  int32_t total_used_credit;
  // at which total credit, we sent the credit to the other side
  int32_t credit_used_checkpoint;
  // credit of the peer as we know it
  // when we transmit a message, we reduce the peer credit until
  // the peer notifies us with its new credit
  int32_t peer_credit;
  // if a write is waiting for credit
  bool waiting_for_credit;
  // an temporary array to hold weather we received a credit message or not
  bool * credit_messages_;
  int postCredit();
  // array of io vectors
  struct iovec *io_vectors;
  // array of tag messages
  struct fi_msg_tagged *tag_messages;
  struct fi_context *recv_contexts;
  struct fi_context *tx_contexts;
  // the remote address
  fi_addr_t	remote_addr;
  // the tag used by this communications
  uint64_t send_tag;
  uint64_t recv_tag;
  uint64_t tag_mask;
  bool started;
};


#endif