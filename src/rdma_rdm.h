#ifndef RDMA_RDM_H_
#define RDMA_RDM_H_

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
#include "rdma_rdm_channel.h"

/**
 * We use tag messagging
 * | 16                   | 16              | 32                 |
 * | Type - control, data | stream id       |                    |
 */

class Datagram {
public:
  Datagram(RDMAOptions *opts,
  struct fi_info *info, struct fid_fabric *fabric,
  struct fid_domain *domain, RDMAEventLoop *loop);
  void Free();

  virtual ~Datagram();

  int start();

  struct fid_ep *GetEp() {
    return ep;
  }

  // disconnect
  int closeConnection();

  // remove end has done a connection close
  int ConnectionClosed();

  uint32_t getPort();

  char *getIPAddress();

  /**
 * Allocate the resources for this connection
 */
  int SetupQueues();

  /**
   * Set and initialize the end point
   */
  int InitEndPoint(fid_ep *ep);

  int setOnWriteComplete(VCallback<uint32_t> onWriteComplete);

  int registerRead(VCallback<int> onWrite);
  int registerWrite(VCallback<int> onWrite);
private:
  // options for initialization
  RDMAOptions *options;
  // fabric information obtained
  struct fi_info *info;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // the fabric
  struct fid_fabric *fabric;
  // fabric domain we are working with
  struct fid_domain *domain;
  // end point
  struct fid_ep *ep, *alias_ep;
  // cq attribute for getting completion notifications
  struct fi_cq_attr cq_attr;
  // transfer cq and receive cq
  struct fid_cq *txcq, *rxcq;
  // receive cq fd and transmit cq fd
  int rx_fd, tx_fd;
  // send and receive contexts
  struct fi_context tx_ctx, rx_ctx;
  // loop info for transmit and recv
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

  // data structures for psm2
  struct fid_av *av;
  struct fi_av_attr av_attr = {
      .type = FI_AV_MAP,
      .count = 1
  };

  RDMAEventLoop *eventLoop;

  /** Private methods */
  int AllocateBuffers(void);
  int TransmitComplete();
  int ReceiveComplete();

  void OnRead(rdma_loop_status state);

  void OnWrite(rdma_loop_status state);

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
  // remote rdm channels
  std::unordered_map<uint32_t, RDMADatagramChannel *> channels;
  // the tag used by control messages
  int64_t control_tag;
};

#endif