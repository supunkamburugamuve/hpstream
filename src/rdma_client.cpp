#include <iostream>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include "rdma_client.h"

RDMAClient::RDMAClient(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoopNoneFD *loop) {
	this->info_hints = rdmaFabric->GetHints();
	this->eventLoop = loop;
	this->options = opts;
	this->eq = NULL;
	this->fabric = rdmaFabric->GetFabric();
	this->info = rdmaFabric->GetInfo();
	this->eq_attr = {};
	this->eq_attr.wait_obj = FI_WAIT_NONE;
	this->con = NULL;
	this->eq_loop.callback = [this](enum rdma_loop_status state) { return this->OnConnect(state); };;
  this->eq_loop.event = CONNECTION;
}

void RDMAClient::Free() {
	HPS_CLOSE_FID(eq);
	HPS_CLOSE_FID(fabric);
}

RDMAConnection* RDMAClient::GetConnection() {
	return this->con;
}

int RDMAClient::OnConnect(enum rdma_loop_status state) {
  struct fi_eq_cm_entry entry;
  uint32_t event;
  ssize_t rd;
  int ret = 0;
  if (state == TRYAGAIN) {
    return 0;
  }
  // read the events for incoming messages
  rd = fi_eq_read(eq, &event, &entry, sizeof entry, 0);
  if (rd == 0) {
    return 0;
  }

  if (rd < 0) {
    return 0;
  }

  if (rd != sizeof entry) {
    HPS_ERR("fi_eq_sread listen");
    return (int) rd;
  }

  if (event == FI_SHUTDOWN) {
    Disconnect();
    return 0;
  } else if (event == FI_CONNECTED) {
    Connected(&entry);
  } else {
    HPS_ERR("Unexpected CM event %d", event);
    ret = -FI_EOTHER;
  }

  return ret;
}

int RDMAClient::Disconnect() {
  return this->con->closeConnection();
}

int RDMAClient::Connect(void) {
	struct fi_eq_cm_entry entry;
	uint32_t event;
	ssize_t rd;
	int ret;
	struct fid_ep *ep = NULL;
	struct fid_domain *domain = NULL;
	RDMAConnection *con = NULL;

	ret = fi_eq_open(this->fabric, &this->eq_attr, &this->eq, NULL);
	if (ret) {
		HPS_ERR("fi_eq_open %d", ret);
		return ret;
	}

	ret = fi_domain(this->fabric, this->info, &domain, NULL);
	if (ret) {
		HPS_ERR("fi_domain %d", ret);
		return ret;
	}

	// create the connection
	con = new RDMAConnection(this->options, this->info, this->fabric, domain, this->eventLoop);

	// allocate the resources
	ret = con->SetupQueues();
	if (ret) {
		return ret;
	}

	// create the end point for this connection
	ret = fi_endpoint(domain, this->info, &ep, NULL);
	if (ret) {
		HPS_ERR("fi_endpoint %d", ret);
		return ret;
	}

	// initialize the endpoint
	ret = con->InitEndPoint(ep, this->eq);
	if (ret) {
		return ret;
	}

	ret = fi_connect(ep, this->info->dest_addr, NULL, 0);
	if (ret) {
		HPS_ERR("fi_connect %d", ret);
		return ret;
	}

  ret = this->eventLoop->RegisterRead(&this->eq_loop);
  if (ret) {
    HPS_ERR("Failed to register event queue fid %d", ret);
    return ret;
  }

  this->con = con;
	return 0;
}

int RDMAClient::Connected(struct fi_eq_cm_entry *entry) {
  int ret;
  if (entry->fid != &(this->con->GetEp()->fid)) {
    ret = -FI_EOTHER;
    return ret;
  }

  // lets start the connection
  if (con->start()) {
    HPS_ERR("Failed to start the connection");
    return 1;
  }

  this->con = con;
  printf("Connection established\n");
  return 0;
}

bool RDMAClient::IsConnected() {
  return con != NULL && con->GetState() == CONNECTED;
}




