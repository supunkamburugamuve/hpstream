#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <map>

#include "rdma_event_loop.h"

RDMAEventLoop::RDMAEventLoop(struct fid_fabric *fabric) {
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

void RDMAEventLoop::Loop() {
  int ret;

  while (run) {
    int size = (int) fids.size();

    memset(events, 0, sizeof events);
//    HPS_INFO("Wait.......... wit size %d", size);
    int trywait = fi_trywait(fabric, fid_list, size);
    if (trywait == FI_SUCCESS) {
//      HPS_INFO("Wait success");
      ret = (int) TEMP_FAILURE_RETRY(epoll_wait(epfd, events, size, -1));
      if (ret < 0) {
        ret = -errno;
        HPS_ERR("epoll_wait %d", ret);
      }
//      HPS_INFO("Epoll wait returned %d size=%d", ret, size);
      for (int j = 0; j < ret; j++) {
        struct epoll_event *event = events + j;
        struct rdma_loop_info *callback = (struct rdma_loop_info *) event->data.ptr;
        if (callback != NULL) {
          IRDMAEventCallback *c = callback->callback;
          c->OnEvent(callback->event, AVAILABLE);
        } else {
          HPS_ERR("Connection NULL");
        }
      }
    } else if (trywait == -FI_EAGAIN){
      for (std::list<struct rdma_loop_info *>::iterator it=connections.begin(); it!=connections.end(); ++it) {
        struct rdma_loop_info *c = *it;
        c->callback->OnEvent(c->event, TRYAGAIN);
      }
    }
  }
}

int RDMAEventLoop::RegisterRead(struct rdma_loop_info *connection) {
  struct epoll_event event;
  int ret;
  int fid = connection->fid;
  HPS_INFO("Register FID %d", fid);
  this->fids.push_back(connection->desc);
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

int RDMAEventLoop::UnRegister(struct rdma_loop_info *con) {
  struct epoll_event event;
  int ret;
  int size;
  int i = 0;
  std::list<struct fid *>::iterator fidIt;
  size = (int) fids.size();
  HPS_INFO("Size of fids before %d", size);
  // remove the fid
  int fid = con->fid;
  ret = epoll_ctl(epfd, EPOLL_CTL_DEL, fid, &event);
  if (ret) {
    ret = -errno;
    HPS_ERR("Failed to un-register connection %d", ret);
    return ret;
  }
  HPS_INFO("Size of fids before 1");
  // remove the fid
  fidIt = fids.begin();
  while (fidIt != fids.end()) {
    struct fid *temp = *fidIt;
    if (temp == con->desc) {
      fids.erase(fidIt);
      break;
    } else {
      fidIt++;
    }
  }

  HPS_INFO("Size of fids before 2");
  // remove the connection
  std::list<struct rdma_loop_info*>::iterator lpIt = connections.begin();
  while (lpIt != connections.end()) {
    struct rdma_loop_info* temp = *lpIt;
    if (temp == con) {
      connections.erase(lpIt);
      break;
    } else {
      fidIt++;
    }
  }
  HPS_INFO("Size of fids before 3");

  // lets re-calculate the lists
  size = (int) fids.size();
  HPS_INFO("Size of fids after %d", size);
  // get all the elements in fids and create a list
  if (fid_list) {
    delete fid_list;
  }
  fid_list = new struct fid*[size];
  i = 0;
  for (std::list<struct fid *>::iterator it=fids.begin(); it!=fids.end(); ++it) {
    fid_list[i] = *it;
    i++;
  }

  return 0;
};




