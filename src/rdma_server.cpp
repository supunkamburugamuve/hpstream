#include <pthread.h>
#include <iostream>
#include <glog/logging.h>
#include "rdma_server.h"
#include "heron_rdma_connection.h"
#include "utils.h"

RDMABaseServer::RDMABaseServer(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoop *loop) {
  this->options = opts;
  this->info_hints = rdmaFabric->GetHints();
  this->eventLoop_ = loop;
  this->pep = NULL;
//  this->info_pep = rdmaFabric->GetInfo();
  this->eq = NULL;
  this->fabric = rdmaFabric->GetFabric();
  this->eq_attr = {};
  this->domain = NULL;
  this->rdmaFabric = rdmaFabric;
  // initialize this attribute, search weather this is correct
  this->eq_attr.wait_obj = FI_WAIT_NONE;
  this->eq_attr.size = 64;

  this->eq_loop.callback = [this](enum rdma_loop_status state) { return this->OnConnect(state); };
  this->eq_loop.event = CONNECTION;
  this->eq_loop.valid = true;
}

RDMABaseServer::RDMABaseServer(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMADatagram *loop) {
  this->options = opts;
  this->info_hints = rdmaFabric->GetHints();
  this->datagram_ = loop;
  this->eventLoop_ = NULL;
  this->pep = NULL;
//  this->info_pep = rdmaFabric->GetInfo();
  this->eq = NULL;
  this->fabric = rdmaFabric->GetFabric();
  this->eq_attr = {};
  this->domain = NULL;
  this->rdmaFabric = rdmaFabric;
  // initialize this attribute, search weather this is correct
  this->eq_attr.wait_obj = FI_WAIT_NONE;
  this->eq_attr.size = 64;

  this->eq_loop.callback = [this](enum rdma_loop_status state) { return this->OnConnect(state); };
  this->eq_loop.event = CONNECTION;
  this->eq_loop.valid = true;

  auto cb = [this](uint32_t stream) { return this->OnRDMConnect(stream); };
  loop->SetRDMConnect(cb);
}

RDMABaseServer::~RDMABaseServer() {}

int RDMABaseServer::Start_Base(void) {
  int ret;
  LOG(INFO) << "Starting the RDMA server";
  ret = hps_utils_get_info_server(options, info_hints, &info_pep);
  if (ret) {
    LOG(ERROR) << "Failed to get server information";
    return ret;
  }

  ret = fi_domain(this->fabric, info_pep, &this->domain, NULL);
  if (ret) {
    LOG(INFO) << "fi_domain " << ret;
    return ret;
  }

  if (options->provider == VERBS_PROVIDER_TYPE) {
    ret = StartAcceptingConnections();
    if (ret) {
      LOG(INFO) << "Failed to start accepting connections";
      return ret;
    }
  }
  return 0;
}

int RDMABaseServer::AddChannel(uint16_t target_id, char *node, char *service) {
  RDMAOptions opt;
  opt.dst_addr = node;
  opt.dst_port = service;
  opt.src_addr = options->src_addr;
  opt.src_port = options->src_port;
  struct fi_info *target;

  int ret = hps_utils_get_info_client(&opt, info_hints, &target);
  if (ret) {
    LOG(ERROR) << "Failed to get client information";
    return ret;
  }

  RDMADatagramChannel *channel_ = datagram_->CreateChannel(target_id, target);
  RDMABaseConnection *con = CreateConnection(channel_, options, this->eventLoop_, READ_ONLY);
  this->active_connections_.insert(con);
  LOG(INFO) << "Created channel to stream id: " << target_id;
  return 0;
}

int RDMABaseServer::OnRDMConnect(uint16_t stream_id) {
  RDMADatagramChannel *channel_ = datagram_->GetChannel(stream_id);
  RDMABaseConnection *con = CreateConnection(channel_, options, this->eventLoop_, READ_ONLY);
  con->start();
  this->active_connections_.insert(con);
  HandleNewConnection_Base(con);
  LOG(INFO) << "Created channel to stream id: " << stream_id;
  return 0;
}

int RDMABaseServer::StartAcceptingConnections() {
  // open the event queue for passive end-point
  int ret = fi_eq_open(this->fabric, &this->eq_attr, &this->eq, NULL);
  if (ret) {
    LOG(INFO) << "fi_eq_open " << ret;
    return ret;
  }

  ret = hps_utils_get_eq_fd(this->options, this->eq, &this->eq_fid);
  if (ret) {
    LOG(ERROR) << "Failed to get event queue fid: " << ret;
    return ret;
  }
  LOG(INFO) << "EQ FID: " << eq_fid;
  this->eq_loop.fid = eq_fid;
  this->eq_loop.desc = &this->eq->fid;

  // allocates a passive end-point
  ret = fi_passive_ep(this->fabric, this->info_pep, &this->pep, NULL);
  if (ret) {
    LOG(INFO) << "fi_passive_ep " << ret;
    return ret;
  }

  // bind the passive end-point to the event queue
  ret = fi_pep_bind(this->pep, &eq->fid, 0);
  if (ret) {
    LOG(INFO) << "fi_pep_bind " << ret;
    return ret;
  }

  ret = this->eventLoop_->RegisterRead(&this->eq_loop);
  if (ret) {
    LOG(INFO) << "Failed to register event queue fid " << ret;
    return ret;
  }
  // start listen for incoming connections
  ret = fi_listen(this->pep);
  if (ret) {
    LOG(INFO) << "fi_listen " << ret;
    return ret;
  }
  LOG(INFO) << "Started listening for incoming RDMA connections";
  return 0;
}

int RDMABaseServer::Stop_Base() {
  LOG(INFO) << "Stopping the server";
  // unregister us from any connection events
  if (eventLoop_) {
    if (!eventLoop_->UnRegister(&eq_loop)) {
      LOG(WARNING) << "Failed to unregistet event loop";
    }
  }

  for (auto it = active_connections_.begin(); it != active_connections_.end(); ++it) {
    RDMABaseConnection* conn = *(it);
    conn->closeConnection();
    // Note:- we don't delete the connection here. They are deleted in
    // the OnConnectionClose call.
    CHECK(active_connections_.empty());
  }

  // free the rdma resources
  HPS_CLOSE_FID(pep);
  HPS_CLOSE_FID(eq);
  HPS_CLOSE_FID(domain);
//  HPS_CLOSE_FID(fabric);
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

  // read the events for incoming messages
  rd = fi_eq_read(eq, &event, &entry, sizeof entry, 0);
  if (rd == 0 || rd == -FI_EAGAIN) {
    return;
  }

  if (rd < 0) {
    if (rd == -FI_EAVAIL) {
      rd = hps_utils_eq_readerr(eq);
      LOG(WARNING) << "Failed to red the eq: " + rd;
    }
    return;
  }

  if (rd != sizeof entry) {
    LOG(INFO) << "Unexpected event received on connection listen "
              << rd << " and expected " << sizeof entry;
    return;
  }

  if (event == FI_SHUTDOWN) {
    LOG(INFO) << "Received shutdown event";
    std::set<RDMABaseConnection *>::iterator it = active_connections_.begin();
    // remove the connection from the list
    while (it != active_connections_.end()) {
      RDMAConnection *rdmaConnection = (RDMAConnection *) (*it)->getEndpointConnection();
      if (&rdmaConnection->GetEp()->fid == entry.fid) {
        HandleConnectionClose_Base(*it, OK);
        rdmaConnection->ConnectionClosed();
        LOG(INFO) << "Closed connection";
        active_connections_.erase(it);
        break;
      }
      it++;
    }

    return;
  } else if (event == FI_CONNREQ) {
    LOG(INFO) << "Received connect event";
    // this is the correct fi_info associated with active end-point
    Connect(&entry);
  } else if (event == FI_CONNECTED) {
    LOG(INFO) << "Received connection completion event";
    Connected(&entry);
  } else {
    LOG(WARNING) << "Unexpected CM event: " << event;
  }
}

int RDMABaseServer::Connect(struct fi_eq_cm_entry *entry) {
  int ret;
  struct fid_ep *ep;
  RDMAConnection *con;
  RDMABaseConnection *baseConnection;

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
  ret = fi_endpoint(domain, entry->info, &ep, NULL);
  if (ret) {
    LOG(ERROR) << "Failed to create endoint for connection " << ret;
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
    LOG(ERROR) << "Failed to accept connection" << ret;
    goto err;
  }

  baseConnection = CreateConnection(con, options, this->eventLoop_, READ_WRITE);
  // add the connection to pending and wait for confirmation
  pending_connections_.insert(baseConnection);
  return 0;
  err:
  LOG(INFO) << "FI reject";
  fi_reject(pep, entry->info->handle, NULL, 0);
  return ret;
}

int RDMABaseServer::Connected(struct fi_eq_cm_entry *entry) {
  // first lets find this in the pending connections
  RDMABaseConnection *con = NULL;
  std::set<RDMABaseConnection *>::iterator it = pending_connections_.begin();
  while (it != pending_connections_.end()) {
    RDMABaseConnection *temp = *it;
    RDMAConnection *rdmaConnection = (RDMAConnection *) temp->getEndpointConnection();
    if (&rdmaConnection->GetEp()->fid == entry->fid) {
      con = temp;
      LOG(INFO) << "Remove from pending " << pending_connections_.size();
      pending_connections_.erase(it);
      LOG(INFO) << "Pending size: " << pending_connections_.size();
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

  LOG(INFO) << "Client connected";
  // add the connection to list
  this->active_connections_.insert(con);
  // ask the inheritors to handle the connect event
  HandleNewConnection_Base(con);
  return 0;
}

void RDMABaseServer::CloseConnection_Base(RDMABaseConnection *_connection) {
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Got the request close an unknown connection " << _connection << "\n";
    return;
  }
  _connection->closeConnection();
  return;
}
