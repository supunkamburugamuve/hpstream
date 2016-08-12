#include <pthread.h>
#include <iostream>
#include "server.h"

static void* loopEventsThread(void *param) {
  Server* server = static_cast<Server *>(param);
  server->loop();
  return NULL;
}

Server::Server(Options *opts, fi_info *hints) {
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
  this->con = NULL;
  this->acceptConnections = true;
}

void Server::Free() {
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

Connection* Server::GetConnection() {
  return this->con;
}

int Server::Start() {
  int ret;

  // start the loop thread
  ret = pthread_create(&loopThreadId, NULL, &loopEventsThread, (void *)this);
  if (ret) {
    HPS_ERR("Failed to create thread %d", ret);
    return ret;
  }

  return 0;
}

int Server::Wait() {
  pthread_join(loopThreadId, NULL);
  return 0;
}

/**
 * Initialize the server
 */
int Server::Init(void) {
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

  this->eventLoop = new EventLoop(fabric);
  ret = this->eventLoop->RegisterRead(this->eq_fid, &eq->fid, this);
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

int Server::OnEvent(int fid, enum loop_status state){
  struct fi_eq_cm_entry entry;
  uint32_t event;
  ssize_t rd;
  int ret = 0;
  HPS_INFO("Waiting for connection");
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
    return 0;
  } else if (event == FI_CONNREQ) {
    // this is the correct fi_info associated with active end-point
    Connect(&entry);
  } else {
    HPS_ERR("Unexpected CM event %d", event);
    ret = -FI_EOTHER;
  }

  return ret;
}

int Server::Connect(struct fi_eq_cm_entry *entry) {
  uint32_t event;
  ssize_t rd;
  int ret;
  struct fid_ep *ep;
  // struct fid_domain *domain;
  Connection *con;

  char *fi_str = fi_tostr(entry->info, FI_TYPE_INFO);
  std::cout << "FI ENTRY" << fi_str << std::endl;

//  ret = fi_domain(this->fabric, entry->info, &domain, NULL);
//  if (ret) {
//    HPS_ERR("fi_domain %d", ret);
//    goto err;
//  }

  // create the connection
  con = new Connection(this->options, this->info_hints,
                       entry->info, this->fabric, domain, this->eq);
  // allocate the queues and counters
  ret = con->AllocateActiveResources();
  if (ret) {
    goto err;
  }

  // create the end point for this connection
  ret = fi_endpoint(domain, entry->info, &ep, NULL);
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

  // read the confirmation
  rd = fi_eq_sread(eq, &event, entry, sizeof (struct fi_eq_cm_entry), -1, 0);
  if (rd != sizeof (struct fi_eq_cm_entry)) {
    HPS_ERR("fi_eq_sread accept %d", (int)rd);
    ret = (int) rd;
    goto err;
  }

  if (event != FI_CONNECTED || entry->fid != &ep->fid) {
    HPS_ERR("Unexpected CM event %d fid %p (ep %p)", event, entry->fid, ep);
    ret = -FI_EOTHER;
    goto err;
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
	ret = this->eventLoop->RegisterRead(con->GetRxFd(), &con->GetRxCQ()->fid, con);
  if (ret) {
    HPS_ERR("Failed to register receive cq to event loop %d", ret);
    return ret;
  }

	ret = this->eventLoop->RegisterRead(con->GetTxFd(), &con->GetTxCQ()->fid, con);
  if (ret) {
    HPS_ERR("Failed to register transmit cq to event loop %d", ret);
    return ret;
  }
  HPS_INFO("Connection established");
  this->con = con;

  // add the connection to list
  this->connections.push_back(con);
  return 0;

  err:
  HPS_INFO("Error label");
  fi_reject(pep, entry->info->handle, NULL, 0);
  return ret;
}

int Server::Disconnect(Connection *con) {
  return con->Disconnect();
}

int Server::loop() {
  if (eventLoop == NULL) {
    HPS_ERR("Event loop not created");
    return 1;
  }
  eventLoop->loop();
  return 0;
}