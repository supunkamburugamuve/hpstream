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

class RDMADatagram {
public:
  RDMADatagram(RDMAOptions *opts, RDMAFabric *fabric, uint32_t stream_id);
  void Free();

  virtual ~RDMADatagram();

  int start();

  struct fid_ep *GetEp() {
    return ep;
  }

  // disconnect
  int closeConnection();

  // remove end has done a connection close
  int ConnectionClosed();

  void Loop();

  /**
 * Allocate the resources for this connection
 */
  int SetupQueues();

  int PostBuffers();
  /**
   * Set and initialize the end point
   */
  int InitEndPoint();

  RDMADatagramChannel* CreateChannel(uint32_t target_id, struct fi_info *target);
  RDMADatagramChannel* GetChannel(uint32_t target_id);
  int SendAddressToRemote(fi_addr_t remote);
  void AddChannel(uint32_t target_id, RDMADatagramChannel *channel);
  void SetRDMConnect(VCallback<uint32_t> connect) {
    this->onRDMConnect = connect;
  };

  int Wait();
  int Stop();
private:
  // options for initialization
  RDMAOptions *options;
  uint32_t stream_id;
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
  struct fi_av_attr av_attr;

  /** Private methods */
  int AllocateBuffers(void);
  int TransmitComplete();
  int ReceiveComplete();

  void OnRead(rdma_loop_status state);

  void OnWrite(rdma_loop_status state);

  int AVInsert(void *addr, size_t count, fi_addr_t *fi_addr,
                             uint64_t flags, void *context);
  /**
 * Transmit a buffer
 */
  ssize_t PostTX(size_t size, int index, fi_addr_t addr, uint32_t send_id, uint16_t type);
  /**
   * Post a receive buffer
   */
  ssize_t PostRX(size_t size, int index);

  int HandleConnect(uint16_t connect_type, int bufer_index, uint32_t target_id);
  int SendConfirmToRemote(fi_addr_t remote);

  // remote rdm channels
  std::unordered_map<uint32_t, RDMADatagramChannel *> channels;
  // the tag used by control messages
  uint64_t recv_tag;
  // the control ignore bits
  uint64_t tag_mask;

  // array of io vectors
  struct iovec *io_vectors;
  // array of tag messages
  struct fi_msg_tagged *tag_messages;
  struct fi_context *recv_contexts;
  struct fi_context *tx_contexts;
  VCallback<uint32_t> onRDMConnect;
  VCallback<uint32_t> onRDMConfirm;

  pthread_t loopThreadId;
  bool run;
};

#endif