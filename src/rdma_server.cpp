#include <pthread.h>
#include <iostream>
#include "rdma_server.h"

RDMAServer::RDMAServer(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoop *loop) {
  this->options = opts;
  this->info_hints = rdmaFabric->GetHints();
  this->eventLoop = loop;
  this->pep = NULL;
  this->info_pep = rdmaFabric->GetInfo();
  this->eq = NULL;
  this->fabric = rdmaFabric->GetFabric();
  this->eq_attr = {};
  this->domain = NULL;
  // initialize this attribute, search weather this is correct
  this->eq_attr.wait_obj = FI_WAIT_UNSPEC;

  this->eq_loop.callback = [this](enum rdma_loop_event event, enum rdma_loop_status state) { return this->OnEvent(event, state); };
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

/**
 * Initialize the server
 */
int RDMAServer::Init(void) {
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

  ret = hps_utils_get_eq_fd(this->options, this->eq, &this->eq_fid);
  if (ret) {
    HPS_ERR("Failed to get event queue fid %d", ret);
    return ret;
  }
  this->eq_loop.fid = eq_fid;
  this->eq_loop.desc = &eq->fid;

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

  ret = this->eventLoop->RegisterRead(&this->eq_loop);
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
    std::list<RDMAConnection *>::const_iterator iterator;
    RDMAConnection *c = (RDMAConnection *) entry.fid->context;
    if (c != NULL) {
      // lets remove from the event loop
      if (eventLoop->UnRegister(c->GetRxLoop())) {
        HPS_ERR("Failed to un-register read from loop");
      }

      if (eventLoop->UnRegister(c->GetTxLoop())) {
        HPS_ERR("Failed to un-register transmit from loop");
      }
      // now disconnect
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
  int ret;
  struct fid_ep *ep;
  RDMAConnection *con;

  // create the connection
  con = new RDMAConnection(this->options, this->info_hints,
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

  con->SetState(WAIT_CONFIRM);
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
  char *peer_host;
  int peer_port;
  // first lets find this in the pending connections
  RDMAConnection *con = NULL;
  std::list<RDMAConnection *>::iterator it = pending_connections.begin();
  while (it != pending_connections.end()) {
    RDMAConnection *temp = *it;
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

  ret = con->SetupBuffers();
  if (ret) {
    HPS_ERR("Failed to set up the buffers %d", ret);
    return ret;
  }

  // registe with the loop
  HPS_INFO("RXfd=%d TXFd=%d", con->GetRxFd(), con->GetTxFd());
  rx_loop = con->GetRxLoop();
  ret = this->eventLoop->RegisterRead(rx_loop);
  if (ret) {
    HPS_ERR("Failed to register receive cq to event loop %d", ret);
    return ret;
  }

  tx_loop = con->GetTxLoop();
  ret = this->eventLoop->RegisterRead(tx_loop);
  if (ret) {
    HPS_ERR("Failed to register transmit cq to event loop %d", ret);
    return ret;
  }

  peer_host = con->getIPAddress();
  peer_port = con->getPort();
  HPS_INFO("Connection established %s %d", peer_host, peer_port);
  con->SetState(CONNECTED);
  // add the connection to list
  this->connections.push_back(con);
  return 0;
}

int RDMAServer::Disconnect(RDMAConnection *con) {
  return con->Disconnect();
}
