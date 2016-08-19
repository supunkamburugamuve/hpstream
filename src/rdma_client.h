#ifndef RDMA_CLIENT_H_
#define RDMA_CLIENT_H_

#include "utils.h"
#include "options.h"
#include "rdma_connection.h"
#include "rdma_event_loop.h"
#include "rdma_fabric.h"

class RDMAClient {
public:
  RDMAClient(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoop *loop);
  int Connect(void);
  RDMAConnection *GetConnection();
  void Free();
  int OnConnect(enum rdma_loop_status state);
private:
  // options for initialization
  RDMAOptions *options;
  // fabric information obtained
  struct fi_info *info;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // the event queue to for  connection handling
  struct fid_eq *eq;
  // the loop callback
  struct rdma_loop_info eq_loop;
  // the file descriptor for eq
  int eq_fid;
  // event queue attribute
  struct fi_eq_attr eq_attr;
  // the fabric
  struct fid_fabric *fabric;
  // the connection
  RDMAConnection *con;

  RDMAEventLoop *eventLoop;
  // looping thread id
  pthread_t loopThreadId;

  int Disconnect();
};

#endif /* SCLIENT_H_ */