#include <cstdio>
#include <cstdlib>

#include "options.h"
#include "utils.h"

RDMAOptions::RDMAOptions() {
  this->dst_addr = NULL;
  this->dst_port = NULL;
  this->src_addr = NULL;
  this->src_port = NULL;
  this->options = HPS_OPT_RX_CQ | HPS_OPT_TX_CQ;
  this->rma_op = HPS_RMA_WRITE;
  this->comp_method = HPS_COMP_SPIN;
  this->buf_size = 0;
  this->no_buffers = 4;
}

void RDMAOptions::Free() {
  if (this->dst_addr) {
    free(this->dst_addr);
  }
  if (this->dst_port) {
    free(this->dst_port);
  }
  if (this->src_addr) {
    free(this->src_addr);
  }
}