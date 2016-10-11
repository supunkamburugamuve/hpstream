#include "hps_utils.h"
#include "heron_client.h"
#include <ctime>
#include <unistd.h>

struct timespec start, end_t;
RDMAConnection *con;
RDMAOptions options;
struct fi_info *hints;
RDMAEventLoop *eventLoop;
RDMAFabric *fabric;
StMgrClient *client;

#define ITERATIONS_ 1000000
#define SIZE_ 10000
#define BYTES_ (SIZE_ * 4)

int64_t get_elapsed(const struct timespec *b, const struct timespec *a) {
  int64_t elapsed;

  elapsed = (int64_t) (difftime(a->tv_sec, b->tv_sec) * 1000 * 1000 * 1000);
  elapsed += a->tv_nsec - b->tv_nsec;
  return elapsed / (1000);
}

int connect3() {
  fabric = new RDMAFabric(&options, hints);
  fabric->Init();
  eventLoop = new RDMAEventLoop(fabric->GetFabric());
  client = new StMgrClient(eventLoop, &options, fabric);
  client->Start_base();
  eventLoop->Start();
  while (!client->IsConnected());
  sleep(2);
  return 1;
}

int exchange3() {
  Timer timer;
  for (int i = -1; i < 1000000; i++) {
    char *name = new char[100];
    sprintf(name, "Helooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo");
    proto::stmgr::TupleMessage *message = new proto::stmgr::TupleMessage();
    message->set_name(name);
    message->set_id(i);
    message->set_data(name);
    message->set_time(timer.currentTime());
    client->SendTupleStreamMessage(message);
    delete name;
  }
  eventLoop->Wait();
  return 0;
}

int main(int argc, char **argv) {
  int op;
  options.buf_size = 1024 * 64;
  options.no_buffers = 10;
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
  connect3();
  exchange3();
  return 0;
}
