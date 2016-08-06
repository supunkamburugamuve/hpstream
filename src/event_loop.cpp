#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include "event_loop.h"

struct connect_info {
  Connection *con;
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
  struct epoll_event event;
  while (run) {
    unsigned long size = fids.size();
    if (size == 0) {
      pthread_yield();
      continue;
    }
    // get all the elements in fids and create a list
    struct fid **fid_list = new struct fid*[size];
    int i = 0;
    for ( auto it = this->fids.begin(); it != this->fids.end(); ++it ) {
      fid_list[i++] = it->second;
    }

    memset(&event, 0, sizeof event);
    // HPS_INFO("Wait..........");
    if (fi_trywait(fabric, fid_list, 1) == FI_SUCCESS) {
      HPS_INFO("Wait success");
      ret = (int) TEMP_FAILURE_RETRY(epoll_wait(epfd, &event, 1, -1));
      if (ret < 0) {
        ret = -errno;
        HPS_ERR("epoll_wait %d", ret);
      }

      struct connect_info *con = (struct connect_info *) event.data.ptr;
      if (con != NULL) {
        Connection *c = con->con;
        int f = con->fid;
        HPS_ERR("Connection fd %d", f);
        c->Ready(f);
      } else {
        HPS_ERR("Connection NULL");
      }
    }

    delete fid_list;
  }
}

int EventLoop::RegisterRead(int fid, struct fid *desc, Connection *connection) {
  struct epoll_event event;
  int ret;
  if (fids.find(fid) == fids.end()) {
    this->fids[fid] = desc;
    struct connect_info *info = new connect_info();
    info->con = connection;
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




