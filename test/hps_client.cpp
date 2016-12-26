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
  serverOptions->buf_size = 64 * 1024;
  serverOptions->no_buffers = 10;
  RDMAFabric *serverFabric = new RDMAFabric(serverOptions);
  serverFabric->Init();
  server = new RDMAStMgrServer(eventLoop, serverOptions, loopFabric, NULL, &timer);
  server->Start();
  server->origin = true;


  RDMAOptions *clientOptions = new RDMAOptions();
  clientOptions->dst_addr = options.dst_addr;
  clientOptions->dst_port = options.dst_port;
  clientOptions->options = 0;
  clientOptions->buf_size = 64 * 1024;
  clientOptions->no_buffers = 10;
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
  for (int i = -1; i < 100000; i++) {
    char *name = new char[100];
    // LOG(INFO) << "Sending message";
    sprintf(name, "Helooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo");
    proto::stmgr::TupleMessage *message = new proto::stmgr::TupleMessage();
    message->set_name(name);
    message->set_id(i);
    message->set_data(name);
    message->set_time(timer.currentTime());
    client->SendTupleStreamMessage(message);
    delete []name;
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

//  eventLoop->Close();
  client->Quit();
//  delete client;
//  server->Stop();
//  delete server;
//  delete eventLoop;
  exit(0);
}

int main(int argc, char **argv) {
  int op;
  options.buf_size = 1024 * 64;
  options.no_buffers = 10;
  signal(SIGINT, INThandler);
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
