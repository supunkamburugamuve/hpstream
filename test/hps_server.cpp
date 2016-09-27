#include "hps_utils.h"
#include "heron_stmgr_server.h"

RDMAOptions options;
struct fi_info *hints;
StMgrServer *server;
RDMAEventLoopNoneFD *eventLoop;
RDMAFabric *fabric;

#define ITERATIONS_ 1000000
#define SIZE_ 10000
#define BYTES_ (SIZE_ * 4)

int connect() {
  int ret = 0;
  fabric = new RDMAFabric(&options, hints);
  fabric->Init();
  eventLoop = new RDMAEventLoopNoneFD(fabric->GetFabric());

  server = new StMgrServer(eventLoop, &options, fabric);
  server->Start();
  eventLoop->Start();
  eventLoop->Wait();
  return ret;
}

int main(int argc, char **argv) {
  int op;
  options.buf_size = 1024 * 60;
  options.no_buffers = 6;
  hints = fi_allocinfo();
  // parse the options
  while ((op = getopt(argc, argv, "ho:" ADDR_OPTS INFO_OPTS)) != -1) {
    switch (op) {
      default:
        rdma_parseinfo(op, optarg, hints);
        rdma_parse_addr_opts(op, optarg, &options);
        break;
      case '?':
      case 'h':
        fprintf(stderr, "Help not implemented\n");
        return 0;
    }
  }

  if (optind < argc) {
    options.dst_addr = argv[optind];
    printf("dst addr: %s\n", options.dst_addr);
  }

  hints->ep_attr->type = FI_EP_MSG;
  hints->caps = FI_MSG | FI_RMA;
  hints->mode = FI_LOCAL_MR | FI_RX_CQ_DATA;
  print_info(hints);
  connect();
  return 0;
}
