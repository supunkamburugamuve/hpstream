#include "hps_utils.h"
#include "heron_stmgr_server.h"

RDMAOptions options;
StMgrServer *server;
RDMAEventLoopNoneFD *eventLoop;
RDMAFabric *fabric;

#define ITERATIONS_ 1000000
#define SIZE_ 10000
#define BYTES_ (SIZE_ * 4)

int connect() {
  int ret = 0;
  fabric = new RDMAFabric(&options);
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
  options.buf_size = 1024 * 64;
  options.no_buffers = 10;
  // parse the options
  while ((op = getopt(argc, argv, "ho:" ADDR_OPTS INFO_OPTS)) != -1) {
    switch (op) {
      default:
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

  connect();
  return 0;
}
