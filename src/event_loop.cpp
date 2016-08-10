#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <map>

#include "event_loop.h"

struct loop_info {
  IEventCallback *callback;
  int fid;
};

EventLoop::EventLoop(struct fid_fabric *fabric) {
  int ret;
  this->fabric = fabric;
  this->run = true;

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
    // get all the elements in fids and create a list
    // HPS_INFO("Size of the fids %d", size);
    struct fid **fid_list = new struct fid*[size];
    int i = 0;
    for (std::unordered_map<int,struct fid *>::iterator it=fids.begin(); it!=fids.end(); ++it) {
      fid_list[i] = it->second;
      i++;
    }

    struct epoll_event* events = new struct epoll_event [size];
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
          int f = callback->fid;
          // HPS_ERR("Connection fd %d", f);
          c->OnEvent(f, AVIALBLE);
        } else {
          HPS_ERR("Connection NULL");
        }
      }
    } else if (trywait == -FI_EAGAIN){
      for (std::unordered_map<int, IEventCallback *>::iterator it=connections.begin(); it!=connections.end(); ++it) {
        IEventCallback *c = it->second;
        HPS_ERR("Wait try again Connection fd %d", it->first);
        c->OnEvent(it->first, TRYAGAIN);
      }
    }
    delete fid_list;
    delete events;
  }
}

int EventLoop::RegisterRead(int fid, struct fid *desc, IEventCallback *connection) {
  struct epoll_event event;
  int ret;
  if (fids.find(fid) == fids.end()) {
    HPS_INFO("Register FID %d", fid);
    this->fids[fid] = desc;
    this->connections[fid] = connection;

    struct loop_info *info = new loop_info();
    info->callback = connection;
    info->fid = fid;
    event.data.ptr = (void *)info;
    event.events = EPOLLIN;
    ret = epoll_ctl(epfd, EPOLL_CTL_ADD, fid, &event);
    if (ret) {
      ret = -errno;
      HPS_ERR("epoll_ctl %d", ret);
      return ret;
    }
  } else {
    return 1;
  }
  // create a list of fids
  return 0;
}

int EventLoop::UnRegister(int fid) {
  struct epoll_event event;
  int ret;
  if (fids.find(fid) != fids.end()) {
    ret = epoll_ctl(epfd, EPOLL_CTL_DEL, fid, &event);
    if (ret) {
      ret = -errno;
      HPS_ERR("Failed to un-register connection %d", ret);
      return ret;
    }
  }
  return 0;
};




