#include <pthread.h>
#include <iostream>
#include <glog/logging.h>
#include "rdma_server.h"
#include "connection.h"

RDMABaseServer::RDMABaseServer(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoopNoneFD *loop) {
  this->options = opts;
  this->info_hints = rdmaFabric->GetHints();
  this->eventLoop_ = loop;
  this->pep = NULL;
  this->info_pep = rdmaFabric->GetInfo();
  this->eq = NULL;
  this->fabric = rdmaFabric->GetFabric();
  this->eq_attr = {};
  this->domain = NULL;
  // initialize this attribute, search weather this is correct
  this->eq_attr.wait_obj = FI_WAIT_NONE;

  this->eq_loop.callback = [this](enum rdma_loop_status state) { return this->OnConnect(state); };
  this->eq_loop.event = CONNECTION;
}

RDMABaseServer::~RDMABaseServer() {}

int RDMABaseServer::Start_Base(void) {
  int ret;
  ret = fi_domain(this->fabric, info_pep, &this->domain, NULL);
  if (ret) {
    HPS_ERR("fi_domain %d", ret);
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

  ret = this->eventLoop_->RegisterRead(&this->eq_loop);
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

int RDMABaseServer::Stop_Base() {
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
  return 0;
}

void RDMABaseServer::OnConnect(enum rdma_loop_status state) {
  struct fi_eq_cm_entry entry;
  uint32_t event;
  ssize_t rd;

  if (state == TRYAGAIN) {
    return;
  }
  // read the events for incoming messages
  rd = fi_eq_read(eq, &event, &entry, sizeof entry, 0);
  if (rd == 0 || rd == -EAGAIN) {
    return;
  }

  if (rd < 0) {
    return;
  }

  if (rd != sizeof entry) {
    HPS_ERR("fi_eq_sread listen %ld and expected %ld", rd, sizeof entry);
    return;
  }

  if (event == FI_SHUTDOWN) {
    std::list<RDMAConnection *>::const_iterator iterator;
    RDMAConnection *c = (RDMAConnection *) entry.fid->context;
    if (c != NULL) {
      // now disconnect
      c->closeConnection();
    }
    return;
  } else if (event == FI_CONNREQ) {
    // this is the correct fi_info associated with active end-point
    Connect(&entry);
  } else if (event == FI_CONNECTED) {
    Connected(&entry);
  } else {
    HPS_ERR("Unexpected CM event %d", event);
  }
}

int RDMABaseServer::Connect(struct fi_eq_cm_entry *entry) {
  int ret;
  struct fid_ep *ep;
  RDMAConnection *con;
  BaseConnection *baseConnection;

  // create the connection
  con = new RDMAConnection(this->options, entry->info,
                           this->fabric, domain, this->eventLoop_);
  // allocate the queues and counters
  ret = con->SetupQueues();
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

  con->SetState(WAIT_CONNECT_CONFIRM);

  baseConnection = CreateConnection(con, options, this->eventLoop_);
  // add the connection to pending and wait for confirmation
  pending_connections_.insert(baseConnection);
  return 0;
  err:
  HPS_INFO("Error label");
  fi_reject(pep, entry->info->handle, NULL, 0);
  return ret;
}

int RDMABaseServer::Connected(struct fi_eq_cm_entry *entry) {
  // first lets find this in the pending connections
  BaseConnection *con = NULL;
  std::set<BaseConnection *>::iterator it = pending_connections_.begin();
  while (it != pending_connections_.end()) {
    BaseConnection *temp = *it;
    RDMAConnection *rdmaConnection = temp->getEndpointConnection();
    if (&rdmaConnection->GetEp()->fid == entry->fid) {
      con = temp;
      pending_connections_.erase(it);
      break;
    }
    it++;
  }

  // we didn't find this connection in pending
  if (con == NULL) {
    LOG(ERROR) << "Connected event received for non-pending connection, ignoring";
    return 0;
  }

  // lets start the connection
  if (con->start()) {
    LOG(ERROR) << "Failed to start the connection";
    return 1;
  }

  HPS_INFO("Client connected");
  // add the connection to list
  this->active_connections_.insert(con);
  return 0;
}

void RDMABaseServer::CloseConnection_Base(BaseConnection *_connection) {
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Got the request close an unknown connection " << _connection << "\n";
    return;
  }
  _connection->closeConnection();
  return;
}
