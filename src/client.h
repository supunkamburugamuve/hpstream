#ifndef CLIENT_H_
#define CLIENT_H_

#include "utils.h"
#include "options.h"
#include "connection.h"
#include "rdma_event_loop.h"

class Client : IRDMAEventCallback {
public:
  Client(Options *opts, fi_info *hints);
  int Connect(void);
  Connection *GetConnection();
  void Free();
  int Start();
  int OnEvent(enum rdma_loop_event event, enum rdma_loop_status state);
  /**
   * Start Loop through the events
   */
  int loop();
private:
  // options for initialization
  Options *options;
  // fabric information obtained
  struct fi_info *info;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // the event queue to for  connection handling
  struct fid_eq *eq;
  // the loop callback
  struct loop_info eq_loop;
  // the file descriptor for eq
  int eq_fid;
  // event queue attribute
  struct fi_eq_attr eq_attr;
  // the fabric
  struct fid_fabric *fabric;
  // the connection
  Connection *con;

  RDMAEventLoop *eventLoop;
  // looping thread id
  pthread_t loopThreadId;

  int Disconnect();
};

#endif /* SCLIENT_H_ */