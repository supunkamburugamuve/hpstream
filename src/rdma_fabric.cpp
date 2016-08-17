
#include "rdma_fabric.h"

RDMAFabric::RDMAFabric(RDMAOptions *options, struct fi_info *info_hints) {
  this->options = options;
  this->info_hints = info_hints;
}

int RDMAFabric::Init() {
  int ret;
  ret = hps_utils_get_info(this->options, this->info_hints, &this->info);
  if (ret) {
    return ret;
  }

  ret = fi_fabric(this->info->fabric_attr, &this->fabric, NULL);
  if (ret) {
    HPS_ERR("fi_fabric %d", ret);
    return ret;
  }

  return ret;
}
