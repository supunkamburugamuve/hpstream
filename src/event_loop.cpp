#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <map>

#include "event_loop.h"

EventLoop::EventLoop(struct fid_fabric *fabric) {
  int ret;
  this->fabric = fabric;
  this->run = true;
  this->fid_list = NULL;
  this->events = NULL;
  epfd = epoll_create1(0);
  if (epfd < 0) {
    ret = -errno;
    HPS_ERR("epoll_create1", ret);
    throw ret;
  }
}

void EventLoop::loop() {
  int ret;

  while (run) {
    int size = (int) fids.size();

    memset(events, 0, sizeof events);
    HPS_INFO("Wait.......... wit size %d", size);
    int trywait = fi_trywait(fabric, fid_list, size);
    if (trywait == FI_SUCCESS) {
      HPS_INFO("Wait success");
      ret = (int) TEMP_FAILURE_RETRY(epoll_wait(epfd, events, size, -1));
      if (ret < 0) {
        ret = -errno;
        HPS_ERR("epoll_wait %d", ret);
      }
      HPS_INFO("Epoll wait returned %d size=%d", ret, size);
      for (int j = 0; j < ret; j++) {
        struct epoll_event *event = events + j;
        struct loop_info *callback = (struct loop_info *) event->data.ptr;
        if (callback != NULL) {
          IEventCallback *c = callback->callback;
          c->OnEvent(callback->event, AVIALBLE);
        } else {
          HPS_ERR("Connection NULL");
        }
      }
    } else if (trywait == -FI_EAGAIN){
      for (std::list<struct loop_info *>::iterator it=connections.begin(); it!=connections.end(); ++it) {
        struct loop_info *c = *it;
        c->callback->OnEvent(c->event, TRYAGAIN);
      }
    }
  }
}

int EventLoop::RegisterRead(struct fid *desc, struct loop_info *connection) {
  struct epoll_event event;
  int ret;
  int fid = connection->fid;
  HPS_INFO("Register FID %d", fid);
  this->fids.push_back(desc);
  this->connections.push_back(connection);

  int size = (int) fids.size();
  // get all the elements in fids and create a list
  if (fid_list) {
    delete fid_list;
  }
  fid_list = new struct fid*[size];
  int i = 0;
  for (std::list<struct fid *>::iterator it=fids.begin(); it!=fids.end(); ++it) {
    fid_list[i] = *it;
    i++;
  }

  if (events) {
    delete events;
  }
  events = new struct epoll_event [size];

  event.data.ptr = (void *)connection;
  event.events = EPOLLIN;
  ret = epoll_ctl(epfd, EPOLL_CTL_ADD, fid, &event);
  if (ret) {
    ret = -errno;
    HPS_ERR("epoll_ctl %d", ret);
    return ret;
  }
  return 0;
}

int EventLoop::UnRegister(int fid) {
  struct epoll_event event;
  int ret;
  ret = epoll_ctl(epfd, EPOLL_CTL_DEL, fid, &event);
  if (ret) {
    ret = -errno;
    HPS_ERR("Failed to un-register connection %d", ret);
    return ret;
  }
  return 0;
};




