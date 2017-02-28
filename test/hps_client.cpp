#include "hps_utils.h"
#include "heron_client.h"
#include "heron_stmgr_server.h"
#include <ctime>
#include <unistd.h>

struct timespec start, end_t;
RDMAOptions options;
RDMAEventLoop *eventLoop;
RDMAFabric *loopFabric;
RDMAStMgrClient *client;
RDMAStMgrServer *server;
Timer timer;

#define SIZE_ 10000

int64_t get_elapsed(const struct timespec *b, const struct timespec *a) {
  int64_t elapsed;

  elapsed = (int64_t) (difftime(a->tv_sec, b->tv_sec) * 1000 * 1000 * 1000);
  elapsed += a->tv_nsec - b->tv_nsec;
  return elapsed / (1000);
}

int connect3() {
  loopFabric = new RDMAFabric(&options);
  loopFabric->Init();
  eventLoop = new RDMAEventLoop(loopFabric);
  eventLoop->Start();

  RDMAOptions *serverOptions = new RDMAOptions();
  serverOptions->src_port = options.src_port;
  serverOptions->src_addr = options.src_addr;
  serverOptions->options = 0;
  serverOptions->buf_size = BUFFER_SIZE;
  serverOptions->no_buffers = BUFFERS;
  RDMAFabric *serverFabric = new RDMAFabric(serverOptions);
  serverFabric->Init();
  server = new RDMAStMgrServer(eventLoop, serverOptions, loopFabric, NULL, &timer);
  server->Start();
  server->origin = true;


  RDMAOptions *clientOptions = new RDMAOptions();
  clientOptions->dst_addr = options.dst_addr;
  clientOptions->dst_port = options.dst_port;
  clientOptions->options = 0;
  clientOptions->buf_size = BUFFER_SIZE;
  clientOptions->no_buffers = BUFFERS;
  RDMAFabric *clientFabric = new RDMAFabric(clientOptions);
  clientFabric->Init();

  LOG(INFO) << "Started server";
  client = new RDMAStMgrClient(eventLoop, clientOptions, clientFabric);
  client->Start();
  LOG(INFO) << "Started client";

  while (!client->IsConnected()) {
    sleep(1);
  }
  LOG(INFO) << "Server connected";
  return 1;
}

int exchange3() {
  sleep(2);
  timer.reset();
  std::string name(500000, '0');

  for (int i = -1; i < 10000000; i++) {
    proto::stmgr::TupleMessage *message = new proto::stmgr::TupleMessage();
    message->set_name(name);
    message->set_id(0);
    message->set_data(name);
    message->set_time(timer.currentTime());
    // LOG(INFO) << "Sending message";
    client->SendTupleStreamMessage(message);
  }
  eventLoop->Wait();
  return 0;
}

int exchange4() {
  Timer timer;
  for (int i = -1; i < 1000000; i++) {
    client->SendHelloRequest();
  }
  eventLoop->Wait();
  return 0;
}

void  INThandler(int sig) {
  char  c;

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
    options.dst_addr = strdup(argv[optind]);
    printf("dst addr: %s\n", options.dst_addr);
  }

  connect3();
  exchange3();
  return 0;
}
