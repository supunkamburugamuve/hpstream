#include "hps_utils.h"
#include "heron_client.h"
#include "heron_stmgr_server.h"

RDMAOptions options;
RDMAEventLoop *eventLoop;
RDMAFabric *loopFabric;
RDMAStMgrClient *client;
RDMAStMgrServer *server;
Timer timer;

#define SIZE_ 10000

int connect() {
  int ret = 0;
  loopFabric = new RDMAFabric(&options);
  loopFabric->Init();
  eventLoop = new RDMAEventLoop(loopFabric);
  eventLoop->Start();

  RDMAOptions *clientOptions = new RDMAOptions();
  clientOptions->dst_addr = options.dst_addr;
  clientOptions->dst_port = options.dst_port;
  clientOptions->options = 0;
  clientOptions->buf_size = BUFFER_SIZE;
  clientOptions->no_buffers = BUFFERS;

  RDMAOptions *serverOptions = new RDMAOptions();
  serverOptions->src_port = options.src_port;
  serverOptions->src_addr = options.src_addr;
  serverOptions->options = 0;
  serverOptions->buf_size = BUFFER_SIZE;
  serverOptions->no_buffers = BUFFERS;
  RDMAFabric *serverFabric = new RDMAFabric(serverOptions);
  serverFabric->Init();
  server = new RDMAStMgrServer(eventLoop, serverOptions, loopFabric, clientOptions, &timer);
  server->Start();
  server->origin = false;

  LOG(INFO) << "Started server";
  eventLoop->Wait();
  return ret;
}

void  INThandler(int sig) {
  char  c;
  printf("Signal handler");
  signal(sig, SIG_IGN);

//  client->Quit();
//  delete client;
//  server->Stop();
//  delete server;
//
//  eventLoop->Stop();
//  delete eventLoop;

  exit(0);
}


int main(int argc, char **argv) {
  int op;
  options.buf_size = BUFFER_SIZE;
  options.no_buffers = BUFFERS;
//  signal(SIGINT, INThandler);
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
