#include <iostream>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>
#include <glog/logging.h>

#include "rdma_client.h"
#include "connection.h"

RDMABaseClient::RDMABaseClient(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoopNoneFD *loop) {
	this->info_hints = rdmaFabric->GetHints();
	this->eventLoop_ = loop;
	this->options = opts;
	this->eq = NULL;
	this->fabric = rdmaFabric->GetFabric();
	this->info = rdmaFabric->GetInfo();
	this->eq_attr = {};
	this->eq_attr.wait_obj = FI_WAIT_NONE;
	this->conn_ = NULL;
	this->eq_loop.callback = [this](enum rdma_loop_status state) { return this->OnConnect(state); };;
  this->eq_loop.event = CONNECTION;
	this->state_ = INIT;

	int ret = this->eventLoop_->RegisterRead(&this->eq_loop);
	if (ret) {
		LOG(ERROR) << "Failed to register event queue fid" << ret;
	}
}

void RDMABaseClient::OnConnect(enum rdma_loop_status state) {
	if (state == INIT) {
		return;
	}

  struct fi_eq_cm_entry entry;
  uint32_t event;
  ssize_t rd;
  if (state == TRYAGAIN) {
    return;
  }
  // read the events for incoming messages
  rd = fi_eq_read(eq, &event, &entry, sizeof entry, 0);
  if (rd == 0) {
    return;
  }

  if (rd < 0) {
    return;
  }

  if (rd != sizeof entry) {
    HPS_ERR("fi_eq_sread listen");
  }

  if (event == FI_SHUTDOWN) {
		Stop_base();
  } else if (event == FI_CONNECTED) {
    Connected(&entry);
  } else {
    HPS_ERR("Unexpected CM event %d", event);
  }
}

int RDMABaseClient::Stop_base() {
	if (this->state_ != CONNECTED || this->state_ != CONNECTING) {
		LOG(ERROR) << "Trying to stop an un-connected client";
		return 0;
	}

  this->connection_->closeConnection();
	HPS_CLOSE_FID(eq);
	HPS_CLOSE_FID(fabric);
	this->state_ = DISCONNECTED;
	return 0;
}

int RDMABaseClient::Start_base(void) {
	int ret;
	struct fid_ep *ep = NULL;
	struct fid_domain *domain = NULL;
	RDMAConnection *con = NULL;

	if (state_ != INIT) {
		LOG(ERROR) << "Failed to start connection not in INIT state";
		return -1;
	}

	ret = fi_eq_open(this->fabric, &this->eq_attr, &this->eq, NULL);
	if (ret) {
		LOG(ERROR) << "fi_eq_open %d" << ret;
		return ret;
	}

	ret = fi_domain(this->fabric, this->info, &domain, NULL);
	if (ret) {
		LOG(ERROR) << "fi_domain " << ret;
		return ret;
	}

	// create the connection
	con = new RDMAConnection(this->options, this->info, this->fabric, domain, this->eventLoop_);

	// allocate the resources
	ret = con->SetupQueues();
	if (ret) {
		return ret;
	}

	// create the end point for this connection
	ret = fi_endpoint(domain, this->info, &ep, NULL);
	if (ret) {
		LOG(ERROR) << "fi_endpoint" << ret;
		return ret;
	}

	// initialize the endpoint
	ret = con->InitEndPoint(ep, this->eq);
	if (ret) {
		return ret;
	}

	ret = fi_connect(ep, this->info->dest_addr, NULL, 0);
	if (ret) {
		LOG(ERROR) << "fi_connect %d" << ret;
		return ret;
	}

	this->state_ = CONNECTING;
	this->conn_ = CreateConnection(con, options, this->eventLoop_);
  this->connection_ = con;
	return 0;
}

int RDMABaseClient::Connected(struct fi_eq_cm_entry *entry) {
	if (state_ != CONNECTING) {
		LOG(ERROR) << "Failed to connect a client not in connecting state";
		return -1;
	}

  int ret;
  if (entry->fid != &(this->connection_->GetEp()->fid)) {
    ret = -FI_EOTHER;
    return ret;
  }

  if (conn_->start()) {
    LOG(ERROR) << "Failed to start the connection";
    return 1;
  }

  this->conn_ = conn_;
	this->state_ = CONNECTED;
  printf("Connection established\n");
  return 0;
}

bool RDMABaseClient::IsConnected() {
  return conn_ != NULL && connection_->GetState() == CONNECTED;
}




