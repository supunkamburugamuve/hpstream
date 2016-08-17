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
  fid_t desc;
  enum rdma_loop_event event;
};


class RDMAEventLoop {
public:
  RDMAEventLoop(struct fid_fabric *fabric);
  int RegisterRead(struct rdma_loop_info *connection);
  void Loop();
  int UnRegister(struct rdma_loop_info *con);
  int Start();
  int Wait();
private:
  bool run;
  struct fid_fabric *fabric;
  int epfd;
  struct fid **fid_list;
  struct epoll_event* events;
  pthread_t loopThreadId;
  std::list<struct fid *> fids;
  std::list<struct rdma_loop_info *> connections;
};

#endif