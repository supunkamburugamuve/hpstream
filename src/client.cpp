#include <iostream>
#include <cstdio>
#include <cstdlib>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include "client.h"

Client::Client(Options *opts, fi_info *hints) {
	this->info_hints = hints;
	this->options = opts;
	this->eq = NULL;
	this->fabric = NULL;
	this->eq_attr = {};
	this->eq_attr.wait_obj = FI_WAIT_UNSPEC;
	this->con = NULL;
}

void Client::Free() {
	HPS_CLOSE_FID(eq);
	HPS_CLOSE_FID(fabric);
}

Connection* Client::GetConnection() {
	return this->con;
}

int Client::Connect(void) {
	struct fi_eq_cm_entry entry;
	uint32_t event;
	ssize_t rd;
	int ret;
	struct fid_ep *ep = NULL;
	struct fid_domain *domain = NULL;
	Connection *con = NULL;

	HPS_ERR("Client connect");
	ret = hps_utils_get_info(this->options, this->info_hints, &this->info);
	if (ret)
		return ret;

	ret = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
	if (ret) {
		HPS_ERR("fi_fabric %d", ret);
		return ret;
	}

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
	con = new Connection(this->options, this->info_hints,
											 this->info, this->fabric, domain, this->eq);

	// allocate the resources
	ret = con->AllocateActiveResources();
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

	rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		HPS_ERR("fi_eq_sread connect");
		ret = (int) rd;
		return ret;
	}

	if (event != FI_CONNECTED || entry.fid != &ep->fid) {
		HPS_ERR("Unexpected CM event %d fid %p (ep %p)",
						event, entry.fid, ep);
		ret = -FI_EOTHER;
		return ret;
	}

	// noe exchange keys
	if ((ret = this->con->ExchangeClientKeys())) {
		HPS_ERR("Failed to exchange keys with server");
		return ret;
	}

	this->con = con;
	printf("Connection established\n");
	return 0;
}

