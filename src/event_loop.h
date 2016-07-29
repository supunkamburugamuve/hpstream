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

#include "event_loop.h"

class EventLoop {
public:
  EventLoop();
  int RegisterRead(int fid);
  int RegisterWrite(int fid);
  void loop();
private:
  int epfd;
  std::unordered_map<int,std::string> fds;
};

#endif