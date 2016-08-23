#include <rdma/fi_eq.h>
#include <map>

#include "rdma_event_loop.h"

static void* loopEventsThreadNonBlock(void *param) {
  RDMAEventLoopNoneFD* server = static_cast<RDMAEventLoopNoneFD *>(param);
  server->Loop();
  return NULL;
}

RDMAEventLoopNoneFD::RDMAEventLoopNoneFD(struct fid_fabric *fabric) {
  this->run = true;
}

int RDMAEventLoopNoneFD::RegisterRead(struct rdma_loop_info *connection) {
  this->connections.push_back(connection);
  return 0;
}

void RDMAEventLoopNoneFD::Loop() {
  while (run) {
    for (std::list<struct rdma_loop_info *>::iterator it = connections.begin();
         it != connections.end(); ++it) {
      struct rdma_loop_info *c = *it;
      c->callback(AVAILABLE);
    }
  }
}

int RDMAEventLoopNoneFD::Start() {
  int ret;
  // start the loop thread
  ret = pthread_create(&loopThreadId, NULL, &loopEventsThreadNonBlock, (void *)this);
  if (ret) {
    HPS_ERR("Failed to create thread %d", ret);
    return ret;
  }

  return 0;
}

int RDMAEventLoopNoneFD::Wait() {
  pthread_join(loopThreadId, NULL);
  return 0;
}

int RDMAEventLoopNoneFD::UnRegister(struct rdma_loop_info *con) {
  // remove the connection
  std::list<struct rdma_loop_info*>::iterator lpIt = connections.begin();
  while (lpIt != connections.end()) {
    struct rdma_loop_info* temp = *lpIt;
    if (temp == con) {
      connections.erase(lpIt);
      break;
    } else {
      lpIt++;
    }
  }
  return 0;
}



