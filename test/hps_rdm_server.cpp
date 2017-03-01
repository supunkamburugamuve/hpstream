#include "hps_utils.h"
#include "heron_client.h"
#include "heron_stmgr_server.h"

RDMAOptions options;
// RDMAEventLoop *eventLoop;
//RDMAFabric *loopFabric;
RDMAStMgrClient *client;
RDMAStMgrServer *server;
Timer timer;
int streamId = 0;

#define SIZE_ 10000

int connect() {
  options.buf_size = BUFFER_SIZE;
  options.no_buffers = BUFFERS;
  options.provider = PSM2_PROVIDER_TYPE;

  int ret = 0;
//  loopFabric = new RDMAFabric(&options);
//  loopFabric->Init();
  // eventLoop = new RDMAEventLoop(loopFabric);
  // eventLoop->Start();



  RDMAOptions *clientOptions = new RDMAOptions();
  clientOptions->dst_addr = options.dst_addr;
  clientOptions->dst_port = options.dst_port;
  clientOptions->options = 0;
  clientOptions->buf_size = BUFFER_SIZE;
  clientOptions->no_buffers = BUFFERS;
  clientOptions->provider = PSM2_PROVIDER_TYPE;
//  client = new RDMAStMgrClient(datagram, clientOptions, loopFabric);
//  client->Start();

  RDMAOptions *serverOptions = new RDMAOptions();
  serverOptions->src_port = options.src_port;
  serverOptions->src_addr = options.src_addr;
  serverOptions->options = 0;
  serverOptions->buf_size = BUFFER_SIZE;
  serverOptions->no_buffers = BUFFERS;
  serverOptions->provider = PSM2_PROVIDER_TYPE;
  serverOptions->max_connections = 2;
  RDMAFabric *serverFabric = new RDMAFabric(serverOptions);
  serverFabric->Init();
  RDMADatagram *datagram = new RDMADatagram(&options, serverFabric, streamId);
  datagram->start();
  server = new RDMAStMgrServer(datagram, serverOptions, serverFabric, clientOptions, &timer);
  server->Start();
  //server->AddChannel(1, options.dst_addr, options.dst_port);
  server->origin = false;
//  server->setRDMAClient(client);

  LOG(INFO) << "Started server";
  datagram->Wait();
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
      case 'k':
        streamId = std::stoi(optarg);
        printf("Stream id: %d\n", streamId);
        break;
      case '?':
      case 'h':
        fprintf(stderr, "Help not implemented\n");
        return 0;
      default:
        rdma_parse_addr_opts(op, optarg, &options);
        break;
    }
  }

  if (optind < argc) {
    options.dst_addr = argv[optind];
    printf("dst addr: %s\n", options.dst_addr);
  }

  connect();
  return 0;
}
