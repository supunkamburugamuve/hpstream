#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <map>

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

  while (run) {
    int size = (int) fids.size();
    if (size < 2) {
      pthread_yield();
      continue;
    }
    // get all the elements in fids and create a list
    // HPS_INFO("Size of the fids %d", size);
    struct fid **fid_list = (struct fid**) malloc(sizeof(struct fid *) * size);
    int i = 0;
//    for ( auto it = this->fids.begin(); it != this->fids.end(); ++it ) {
//
//    }

    for (std::unordered_map<int,struct fid *>::iterator it=fids.begin(); it!=fids.end(); ++it) {
      fid_list[i] = it->second;
       HPS_INFO("FID_LIST %ld", fid_list[i]->fclass);
      if (fid_list[i] == NULL) {
        HPS_INFO("NULL for %d", i);
        continue;
      }

      i++;
    }

    struct epoll_event* events = (epoll_event *) malloc(sizeof(struct epoll_event) * size);
    for (int k = 0; k < 2; k++) {
      if (fid_list[k] == NULL) {
        HPS_INFO("NULLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL");
      }
    }
    memset(events, 0, sizeof events);
    // HPS_INFO("Wait..........");
    if (fi_trywait(fabric, fid_list, 2) == FI_SUCCESS) {
      // HPS_INFO("Wait success");
      ret = (int) TEMP_FAILURE_RETRY(epoll_wait(epfd, events, 2, -1));
      if (ret < 0) {
        ret = -errno;
        HPS_ERR("epoll_wait %d", ret);
      }
      HPS_INFO("Epoll wait returned %d size=%d", ret, size);
      for (int j = 0; j < ret; j++) {
        struct epoll_event *event = events + j;
        struct connect_info *con = (struct connect_info *) event->data.ptr;
        if (con != NULL) {
          Connection *c = con->con;
          int f = con->fid;
          // HPS_ERR("Connection fd %d", f);
          c->Ready(f);
        } else {
          HPS_ERR("Connection NULL");
        }
      }
    }
    free(fid_list);
    free(events);
//    delete events;
//    delete fid_list;
  }
}

int EventLoop::RegisterRead(int fid, struct fid *desc, Connection *connection) {
  struct epoll_event event;
  int ret;
  if (fids.find(fid) == fids.end()) {
    HPS_INFO("Register FID %d", fid);
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




