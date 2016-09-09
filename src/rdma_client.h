#ifndef RDMA_CLIENT_H_
#define RDMA_CLIENT_H_

#include "utils.h"
#include "options.h"
#include "rdma_connection.h"
#include "rdma_event_loop.h"
#include "rdma_fabric.h"
#include "rdma_base_connecion.h"

class RDMABaseClient {
public:
  enum State { DISCONNECTED = 0, CONNECTING, CONNECTED };
  RDMABaseClient(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoopNoneFD *loop);
  int Start_base(void);
  int Stop_base();
  RDMAConnection *GetConnection();
  void Free();
  void OnConnect(enum rdma_loop_status state);
  bool IsConnected();
protected:
  BaseConnection *conn_;
  // the connection
  RDMAConnection *connection_;
  RDMAEventLoopNoneFD *eventLoop_;
  State state_;
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


  int Connected(fi_eq_cm_entry *entry);
};

#endif /* SCLIENT_H_ */