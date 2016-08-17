#ifndef EVENT_LOOP_H_
#define EVENT_LOOP_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <netdb.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <unordered_map>

#include <rdma/fabric.h>
#include <list>

#include "rdma_event_loop.h"
#include "hps.h"
#include "rdma_connection.h"

enum rdma_loop_status {AVAILABLE, TRYAGAIN};

enum rdma_loop_event {
  CONNECTION,
  CQ_READ,
  CQ_TRANSMIT
};

class IRDMAEventCallback {
public:
  virtual int OnEvent(enum rdma_loop_event event, enum rdma_loop_status state) = 0;
};

struct rdma_loop_info {
  IRDMAEventCallback *callback;
  int fid;
  enum rdma_loop_event event;
};


class RDMAEventLoop {
public:
  RDMAEventLoop(struct fid_fabric *fabric);
  int RegisterRead(struct fid *desc, struct rdma_loop_info *loop);
  void Loop();
  int UnRegister(Connection *con);

private:
  bool run;
  struct fid_fabric *fabric;
  int epfd;
  struct fid **fid_list;
  struct epoll_event* events;

  std::list<struct fid *> fids;
  std::list<struct rdma_loop_info *> connections;

};

#endif