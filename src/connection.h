#ifndef HPS_CONNECTION_H_
#define HPS_CONNECTION_H_

#include <cstdint>
#include <unistd.h>
#include <stdio.h>
#include <stdint-gcc.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include "buffer.h"
#include "utils.h"
#include "options.h"

class Connection {
public:
  Connection(Options *opts, struct fi_info *info_hints,
             struct fi_info *info, struct fid_fabric *fabric,
             struct fid_domain *domain, struct fid_eq *eq);
  void Free();
  /**
   * Allocate the resources for this connection
   */
  int AllocateActiveResources();

  /**
   * Set and initialize the end point
   */
  int InitEndPoint(fid_ep *ep, fid_eq *eq);

  virtual ~Connection();

  /**
   * Exchange keys with the peer
   */
  int ExchangeServerKeys();
  int ExchangeClientKeys();
  int sync();
  /**
   * Post a message
   */
  ssize_t PostRMA(enum hps_rma_opcodes op, size_t size);
  ssize_t PostRMA(enum hps_rma_opcodes op, size_t size, void *buf);
  ssize_t RMA(enum hps_rma_opcodes op, size_t size);

  ssize_t TX(size_t size);
  ssize_t RX(size_t size);
  /**
   * Send the content in the buffer. Use multiple buffers if needed to send
   */
  int WriteData(uint8_t *buf, size_t size);

  /**
   * Write the current buffers
   */
  int WriteBuffers();

  int CopyDataFromBuffer(int buf_no, void *buf, uint32_t size, uint32_t *read) ;

  inline Buffer *RecevBuffer() {
    return this->recv_buf;
  }

  inline Buffer *SendBuffer() {
    return this->send_buf;
  }

  /**
   * Receive content in to the buffer.
   */
  int receive();
  int Finalize(void);
private:
  // options for initialization
  Options *options;
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
  // address vector
  struct fid_av *av;

  // cq attribute for getting completion notifications
  struct fi_cq_attr cq_attr;
  // cntr attribute for getting counter notifications
  struct fi_cntr_attr cntr_attr;
  // vector attribute for getting completion notifications
  struct fi_av_attr av_attr;

  // transfer cq and receive cq
  struct fid_cq *txcq, *rxcq;
  // transfer counter and receive counter
  struct fid_cntr *txcntr, *rxcntr;

  struct fid_wait *waitset;

  // receive fd and transmit fd
  int rx_fd, tx_fd;

  struct fi_context tx_ctx, rx_ctx;

  // buffer used for communication
  uint8_t *buf, *tx_buf, *rx_buf;
  size_t buf_size, tx_size, rx_size;
  Buffer *recv_buf;
  Buffer *send_buf;

  int ft_skip_mr = 0;

  uint64_t remote_cq_data;
  struct fid_mr *mr;
  struct fid_mr no_mr;

  // sequence numbers for messages posted and received
  // transfer sequence number
  uint64_t tx_seq;
  // completed transfer requests
  uint64_t tx_cq_cntr;
  // receives posted
  uint64_t rx_seq;
  // receive completed
  uint64_t rx_cq_cntr;

  // remote address
  fi_addr_t remote_fi_addr;
  // remote address keys
  struct fi_rma_iov remote;

  int timeout;

  ssize_t PostTX(size_t size, struct fi_context* ctx);
  ssize_t PostRX(size_t size, struct fi_context* ctx);
  int GetTXComp(uint64_t total);
  int GetRXComp(uint64_t total);
  int GetCQComp(struct fid_cq *cq, uint64_t *cur,
                uint64_t total, int timeout);
  int FDWaitForComp(struct fid_cq *cq, uint64_t *cur,
                    uint64_t total, int timeout);
  int WaitForCompletion(struct fid_cq *cq, uint64_t *cur,
                        uint64_t total, int timeout);
  int SpinForCompletion(struct fid_cq *cq, uint64_t *cur,
                        uint64_t total, int timeout);
  int ReceiveCompletions(uint64_t min, uint64_t max);
  int SendCompletions(uint64_t min, uint64_t max);
  int AllocMsgs(void);
  int AllocateBuffers(void);


};

#endif /* HPS_CONNECTION_H_ */

