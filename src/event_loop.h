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
#include "connection.h"

class EventLoop {
public:
  EventLoop(struct fid_fabric *fabric);
  int RegisterRead(int fid, struct fid *desc, Connection *connection);
  void loop();
private:
  bool run;
  struct fid_fabric *fabric;
  int epfd;
  std::unordered_map<int, Callback *> callbacks;
  std::unordered_map<int, struct fid *> fids;
};

#endif