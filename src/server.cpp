#include <pthread.h>
#include "server.h"

static void* acceptConnectionsThread(void *param) {
  Server* server = static_cast<Server *>(param);
  while (server->IsAcceptConnection()) {
    if (server->Connect()) {
      HPS_ERR("Failed to accept connection");
    }
  }
  return NULL;
}

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
  // initialize this attribute, search weather this is correct
  this->eq_attr.wait_obj = FI_WAIT_UNSPEC;
  this->con = NULL;
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

/**
 * Initialize the server
 */
int Server::Start(void) {
  int ret;
  printf("Start server\n");
  // info for passive end-point
  ret = hps_utils_get_info(this->options, this->info_hints, &this->info_pep);
  if (ret) {
    return ret;
  }

  // create the fabric for passive end-point
  ret = fi_fabric(this->info_pep->fabric_attr, &fabric, NULL);
  if (ret) {
    HPS_ERR("fi_fabric %d", ret);
    return ret;
  }

  // open the event queue for passive end-point
  ret = fi_eq_open(this->fabric, &this->eq_attr, &this->eq, NULL);
  if (ret) {
    HPS_ERR("fi_eq_open %d", ret);
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

  // start listen for incoming connections
  ret = fi_listen(this->pep);
  if (ret) {
    HPS_ERR("fi_listen %d", ret);
    return ret;
  }

  // now start accept thread
  ret = pthread_create(&acceptThreadId, NULL, &acceptConnectionsThread, (void *)this);
  if (ret) {
    HPS_ERR("Failed to create thread %d", ret);
    return ret;
  }

  return 0;
}

int Server::Connect(void) {
  struct fi_eq_cm_entry entry;
  uint32_t event;
  ssize_t rd;
  int ret;
  struct fid_ep *ep;
  struct fid_domain *domain;
  Connection *con;

  // read the events for incoming messages
  rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
  if (rd != sizeof entry) {
    HPS_ERR("fi_eq_sread listen");
    return (int) rd;
  }

  // this is the correct fi_info associated with active end-point
  if (event != FI_CONNREQ) {
    HPS_ERR("Unexpected CM event %d", event);
    ret = -FI_EOTHER;
    goto err;
  }

  ret = fi_domain(this->fabric, entry.info, &domain, NULL);
  if (ret) {
    HPS_ERR("fi_domain %d", ret);
    goto err;
  }

  // create the connection
  con = new Connection(this->options, this->info_hints,
                       entry.info, this->fabric, domain, this->eq);
  // allocate the queues and counters
  ret = con->AllocateActiveResources();
  if (ret) {
    goto err;
  }

  // create the end point for this connection
  ret = fi_endpoint(domain, entry.info, &ep, NULL);
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
  rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
  if (rd != sizeof entry) {
    HPS_ERR("fi_eq_sread accept %d", (int)rd);
    ret = (int) rd;
    goto err;
  }

  if (event != FI_CONNECTED || entry.fid != &ep->fid) {
    HPS_ERR("Unexpected CM event %d fid %p (ep %p)", event, entry.fid, ep);
    ret = -FI_EOTHER;
    goto err;
  }

  // registe with the loop
  this->eventLoop->RegisterRead(con->GetRxFd(), &con->GetRxCQ()->fid, con);
  this->eventLoop->RegisterRead(con->GetTxFd(), &con->GetTxCQ()->fid, con);

  HPS_INFO("Connection established");
  this->con = con;

  ret = pthread_create(&loopThreadId, NULL, &loopEventsThread, (void *)this);
  if (ret) {
    HPS_ERR("Failed to create thread %d", ret);
    return ret;
  }

  return 0;
  err:
    fi_reject(pep, entry.info->handle, NULL, 0);
    return ret;
}

int Server::loop() {
  if (eventLoop == NULL) {
    HPS_ERR("Event loop not created");
    return 1;
  }
  eventLoop->loop();
  return 0;
}