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
    // throw ret;
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
    for (std::list<struct fid *>::iterator it=fids.begin(); it!=fids.end(); ++it) {
      fid_list[i] = *it;
      i++;
    }

    struct loop_info **events = new struct loop_info*[size];
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
        struct loop_info *event = events[j];
        IEventCallback *c = event->callback;
        if (c != NULL) {
          // HPS_ERR("Connection fd %d", f);
          c->OnEvent(event->event, AVIALBLE);
        } else {
          HPS_ERR("Connection NULL");
        }
      }
    } else if (trywait == -FI_EAGAIN){
      for (std::list<struct loop_info *>::iterator it=connections.begin(); it!=connections.end(); ++it) {
        struct loop_info *loop = *it;
        IEventCallback *c = loop->callback;
        HPS_ERR("Wait try again Connection fd %d", loop->event);
        c->OnEvent(loop->event, TRYAGAIN);
      }
    }
    delete fid_list;
    delete events;
  }
}

int EventLoop::RegisterRead(struct fid *desc, struct loop_info *connection) {
  int ret;
  HPS_INFO("Register FID");
  this->fids.push_back(desc);
  this->connections.push_back(connection);

  ret = fi_poll_add(poll_fd, desc, 0);
  if (ret) {
    ret = -errno;
    HPS_ERR("epoll_ctl %d", ret);
    return ret;
  }
  // create a list of fids
  return 0;
}

int EventLoop::UnRegister(struct fid *desc) {
  struct epoll_event event;
  int ret;
  ret = fi_poll_del(poll_fd, desc, 0);
  if (ret) {
    ret = -errno;
    HPS_ERR("Failed to un-register connection %d", ret);
    return ret;
  }
  return 0;
};




