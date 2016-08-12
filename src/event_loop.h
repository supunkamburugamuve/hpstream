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

#include "event_loop.h"
#include "hps.h"

enum loop_status {AVIALBLE, TRYAGAIN};

enum hps_loop_event {
  CONNECTION,
  CQ_READ,
  CQ_TRANSMIT
};

class IEventCallback {
public:
  virtual int OnEvent(enum hps_loop_event event, enum loop_status state) = 0;
};

struct loop_info {
  IEventCallback *callback;
  void *data;
  enum hps_loop_event event;
};

class EventLoop {
public:
  EventLoop(struct fid_fabric *fabric, struct fid_domain *domain);
  int RegisterRead(struct fid *desc, struct loop_info *connection);
  void loop();
private:
  bool run;
  struct fid_fabric *fabric;
  struct fid_poll *poll_fd;
  fi_poll_attr poll_attr;
  std::list<struct fid *> fids;
  std::list<struct loop_info *> connections;
  int UnRegister(struct fid *desc);
};

#endif