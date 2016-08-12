#ifndef HPS_CONNECTION_H_
#define HPS_CONNECTION_H_

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

#include "buffer.h"
#include "utils.h"
#include "options.h"
#include "event_loop.h"

class Connection : public IEventCallback {
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

  int SetupBuffers();
  /**
   * Send the content in the buffer. Use multiple buffers if needed to send
   */
  int WriteData(uint8_t *buf, uint32_t size);

  int ReadData(uint8_t *buf, uint32_t size, uint32_t *read);

  bool DataAvailableForRead();

  /** Get the receive buffer */
  Buffer *ReceiveBuffer();
  /** GEt the send buffer */
  Buffer *SendBuffer();

  int Ready(int fd);

  struct fid_cq * GetTxCQ() {
    return txcq;
  }

  struct fid_cq * GetRxCQ() {
    return rxcq;
  }

  int GetTxFd() {
    return tx_fd;
  }

  int GetRxFd() {
    return rx_fd;
  }

  struct fid_ep *GetEp() {
    return ep;
  }

  struct loop_info * getRxLoop() {
    return &rx_loop;
  }

  struct loop_info * getTxLoop() {
    return &tx_loop;
  }

  int OnEvent(enum hps_loop_event event, enum loop_status state);

  // disconnect
  int Disconnect();

  uint32_t getPort();

  char *getIPAddress();

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

  // receive cq fd and transmit cq fd
  int rx_fd, tx_fd;
  // send and receive contexts
  struct fi_context tx_ctx, rx_ctx;
  // loop info for transmit and recv
  struct loop_info tx_loop, rx_loop;

  // buffer used for communication
  uint8_t *buf;
  uint8_t *tx_buf, *rx_buf;
  size_t buf_size, tx_size, rx_size;
  Buffer *recv_buf;
  Buffer *send_buf;

  int ft_skip_mr;

  uint64_t remote_cq_data;
  struct fid_mr *mr;
  struct fid_mr no_mr;

  // sequence numbers for messages posted
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

  ssize_t PostTX(size_t size, uint8_t *buf, struct fi_context* ctx);
  ssize_t PostRX(size_t size, uint8_t *buf, struct fi_context* ctx);

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
  int AllocateBuffers(void);
  int TransmitComplete();
  int ReceiveComplete();

};

#endif /* HPS_CONNECTION_H_ */

