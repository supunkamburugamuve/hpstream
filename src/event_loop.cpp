#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <map>
#include <rdma/fi_domain.h>

#include "event_loop.h"

EventLoop::EventLoop(struct fid_fabric *fabric, struct fid_domain *domain) {
  int ret;
  this->fabric = fabric;
  this->run = true;
  this->poll_attr = {};
  this->poll_attr.flags = 0;

  ret = fi_poll_open(domain, &poll_attr, &poll_fd);
  if (ret) {
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

    struct fi_context **events = new struct fi_context*[size];
    memset(events, 0, sizeof events);
    HPS_INFO("Wait.......... wit size %d", size);
    int trywait = fi_trywait(fabric, fid_list, size);
    if (trywait == FI_SUCCESS) {
      HPS_INFO("Wait success");
      ret = (int) TEMP_FAILURE_RETRY(fi_poll(poll_fd, (void **)events, size));
      if (ret < 0) {
        ret = -errno;
        HPS_ERR("epoll_wait %d", ret);
      }
      HPS_INFO("Epoll wait returned %d size=%d", ret, size);
      for (int j = 0; j < ret; j++) {
        struct fi_context *event = events[j];
        struct loop_info *callback = (struct loop_info *) event->internal[0];
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
  int ret;
  if (fids.find(fid) == fids.end()) {
    HPS_INFO("Register FID %d", fid);
    this->fids[fid] = desc;
    this->connections[fid] = connection;

    ret = fi_poll_add(poll_fd, desc, 0);
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

int EventLoop::UnRegister(struct fid *desc) {
  struct epoll_event event;
  int ret;
  if (fids.find(fid) != fids.end()) {
    ret = fi_poll_del(poll_fd, desc, 0);
    if (ret) {
      ret = -errno;
      HPS_ERR("Failed to un-register connection %d", ret);
      return ret;
    }
  }
  return 0;
};




