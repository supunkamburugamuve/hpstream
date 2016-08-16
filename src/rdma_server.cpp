#include <pthread.h>
#include <iostream>
#include "rdma_server.h"

static void* loopEventsThread(void *param) {
  RDMAServer* server = static_cast<RDMAServer *>(param);
  server->loop();
  return NULL;
}

RDMAServer::RDMAServer(RDMAOptions *opts, fi_info *hints) {
  this->options = opts;
  this->info_hints = hints;
  this->pep = NULL;
  this->info_pep = NULL;
  this->eq = NULL;
  this->fabric = NULL;
  this->eq_attr = {};
  this->domain = NULL;
  // initialize this attribute, search weather this is correct
  this->eq_attr.wait_obj = FI_WAIT_UNSPEC;
  this->acceptConnections = true;

  this->eq_loop.callback = this;
  this->eq_loop.event = CONNECTION;
}

void RDMAServer::Free() {
  HPS_CLOSE_FID(pep);
  HPS_CLOSE_FID(eq);
  HPS_CLOSE_FID(fabric);

  if (this->options) {
    options->Free();
  }
  if (this->info_pep) {
    fi_freeinfo(this->info_pep);
    this->info_pep = NULL;
  }
  if (this->info_hints) {
    fi_freeinfo(this->info_hints);
    this->info_hints = NULL;
  }
}

int RDMAServer::Start() {
  int ret;

  // start the loop thread
  ret = pthread_create(&loopThreadId, NULL, &loopEventsThread, (void *)this);
  if (ret) {
    HPS_ERR("Failed to create thread %d", ret);
    return ret;
  }

  return 0;
}

int RDMAServer::Wait() {
  pthread_join(loopThreadId, NULL);
  return 0;
}

/**
 * Initialize the server
 */
int RDMAServer::Init(void) {
  int ret;
  printf("Start server\n");
  // info for passive end-point
  ret = hps_utils_get_info(this->options, this->info_hints, &this->info_pep);
  if (ret) {
    return ret;
  }
  char *fi_str = fi_tostr(this->info_hints, FI_TYPE_INFO);
  std::cout << "FI PEP" << fi_str << std::endl;

  // create the fabric for passive end-point
  ret = fi_fabric(this->info_pep->fabric_attr, &fabric, NULL);
  if (ret) {
    HPS_ERR("fi_fabric %d", ret);
    return ret;
  }

  HPS_INFO("Domain before");
  ret = fi_domain(this->fabric, info_pep, &this->domain, NULL);
  if (ret) {
    HPS_ERR("fi_domain %d", ret);
    return ret;
  }
  HPS_INFO("Domain after");

  // open the event queue for passive end-point
  ret = fi_eq_open(this->fabric, &this->eq_attr, &this->eq, NULL);
  if (ret) {
    HPS_ERR("fi_eq_open %d", ret);
    return ret;
  }

  ret = hps_utils_get_eq_fd(this->options, this->eq, &this->eq_fid);
  if (ret) {
    HPS_ERR("Failed to get event queue fid %d", ret);
    return ret;
  }
  this->eq_loop.fid = eq_fid;

  // allocates a passive end-point
  ret = fi_passive_ep(this->fabric, this->info_pep, &this->pep, NULL);
  if (ret) {
    HPS_ERR("fi_passive_ep %d", ret);
    return ret;
  }

  // bind the passive end-point to the event queue
  ret = fi_pep_bind(this->pep, &eq->fid, 0);
  if (ret) {
    HPS_ERR("fi_pep_bind %d", ret);
    return ret;
  }

  this->eventLoop = new RDMAEventLoop(fabric);
  ret = this->eventLoop->RegisterRead(&eq->fid, &this->eq_loop);
  if (ret) {
    HPS_ERR("Failed to register event queue fid %d", ret);
    return ret;
  }
  // start listen for incoming connections
  ret = fi_listen(this->pep);
  if (ret) {
    HPS_ERR("fi_listen %d", ret);
    return ret;
  }

  return 0;
}

int RDMAServer::OnEvent(enum rdma_loop_event loop_event, enum rdma_loop_status state) {
  struct fi_eq_cm_entry entry;
  uint32_t event;
  ssize_t rd;
  int ret = 0;
  if (state == TRYAGAIN) {
    return 0;
  }
  // read the events for incoming messages
  rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
  if (rd != sizeof entry) {
    HPS_ERR("fi_eq_sread listen");
    return (int) rd;
  }

  if (event == FI_SHUTDOWN) {
    HPS_ERR("Recv shut down, Ignoring");
    std::list<Connection *>::const_iterator iterator;
    for (iterator = connections.begin(); iterator != connections.end(); ++iterator) {
      Connection *con = *iterator;
      struct fid_ep *ep = con->GetEp();

    }
    Connection *c = (Connection *) entry.fid->context;
    if (c != NULL) {
      HPS_INFO("Connection is found TX=%d RX=%d", c->GetTxFd(), c->GetRxCQ());
      // lets remove from the event loop
//      c->Disconnect();
    }
    return 0;
  } else if (event == FI_CONNREQ) {
    // this is the correct fi_info associated with active end-point
    Connect(&entry);
  } else if (event == FI_CONNECTED) {
    Connected(&entry);
  } else {
    HPS_ERR("Unexpected CM event %d", event);
    ret = -FI_EOTHER;
  }

  return ret;
}

int RDMAServer::Connect(struct fi_eq_cm_entry *entry) {
  uint32_t event;
  ssize_t rd;
  int ret;
  struct fid_ep *ep;
  Connection *con;

  // create the connection
  con = new Connection(this->options, this->info_hints,
                       entry->info, this->fabric, domain, this->eq);
  // allocate the queues and counters
  ret = con->AllocateActiveResources();
  if (ret) {
    goto err;
  }

  // create the end point for this connection
  // associate the connection to the context
  ret = fi_endpoint(domain, entry->info, &ep, con);
  if (ret) {
    HPS_ERR("fi_endpoint %d", ret);
    goto err;
  }

  // initialize the EP
  ret = con->InitEndPoint(ep, this->eq);
  if (ret) {
    goto err;
  }

  // accept the incoming connection
  ret = fi_accept(ep, NULL, 0);
  if (ret) {
    HPS_ERR("fi_accept %d", ret);
    goto err;
  }

  // add the connection to pending and wait for confirmation
  pending_connections.push_back(con);
  return 0;

  err:
  HPS_INFO("Error label");
  fi_reject(pep, entry->info->handle, NULL, 0);
  return ret;
}

int RDMAServer::Connected(struct fi_eq_cm_entry *entry) {
  ssize_t rd;
  struct rdma_loop_info *tx_loop;
  struct rdma_loop_info *rx_loop;
  int ret;

  // first lets find this in the pending connections
  Connection *con = NULL;
  std::list<Connection *>::iterator it = pending_connections.begin();
  while (it != pending_connections.end()) {
    Connection *temp = *it;
    if (&temp->GetEp()->fid == entry->fid) {
      con = temp;
      pending_connections.erase(it);
      break;
    } else {
      it++;
    }
  }

  // we didn't find this connection in pending
  if (con == NULL) {
    HPS_ERR("Connected event received for non-pending connection, ignoring");
    return 0;
  }

  con->getIPAddress();
  con->getPort();

  ret = con->SetupBuffers();
  if (ret) {
    HPS_ERR("Failed to set up the buffers %d", ret);
    return ret;
  }

  // registe with the loop
  HPS_INFO("RXfd=%d TXFd=%d", con->GetRxFd(), con->GetTxFd());
  rx_loop = con->GetRxLoop();
  ret = this->eventLoop->RegisterRead(&con->GetRxCQ()->fid, rx_loop);
  if (ret) {
    HPS_ERR("Failed to register receive cq to event loop %d", ret);
    return ret;
  }

  tx_loop = con->GetTxLoop();
  ret = this->eventLoop->RegisterRead(&con->GetTxCQ()->fid, tx_loop);
  if (ret) {
    HPS_ERR("Failed to register transmit cq to event loop %d", ret);
    return ret;
  }
  HPS_INFO("Connection established");

  // add the connection to list
  this->connections.push_back(con);
  return 0;
}

int RDMAServer::Disconnect(Connection *con) {
  return con->Disconnect();
}

int RDMAServer::loop() {
  if (eventLoop == NULL) {
    HPS_ERR("Event loop not created");
    return 1;
  }
  eventLoop->Loop();
  return 0;
}