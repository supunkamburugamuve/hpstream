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
  virtual int OnEvent(void *fid, enum loop_status state) = 0;
};

struct loop_info {
  IEventCallback *callback;
  void *data;
};

class EventLoop {
public:
  EventLoop(struct fid_fabric *fabric, struct fid_domain *domain);
  int RegisterRead(int fid, struct fid *desc, IEventCallback *callback);
  void loop();
private:
  bool run;
  struct fid_fabric *fabric;
  int epfd;
  struct fid_poll *poll_fd;
  fi_poll_attr poll_attr;
  std::unordered_map<int, struct fid *> fids;
  std::unordered_map<int, IEventCallback *> connections;

  int UnRegister(int fid);
};

#endif