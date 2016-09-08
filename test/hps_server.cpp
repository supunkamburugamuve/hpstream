#include "hps_utils.h"

//Connection *con;
RDMAOptions options;
struct fi_info *hints;
RDMABaseServer *server;
RDMAEventLoopNoneFD *eventLoop;
RDMAFabric *fabric;

#define ITERATIONS_ 1000000
#define SIZE_ 10000
#define BYTES_ (SIZE_ * 4)

int connect3() {
  int ret = 0;
  fabric = new RDMAFabric(&options, hints);
  fabric->Init();
  eventLoop = new RDMAEventLoopNoneFD(fabric->GetFabric());

  server = new RDMABaseServer(&options, fabric, eventLoop);
  server->Start_Base();
  eventLoop->Start();
  return ret;
}

int exchange3() {
  int values[SIZE_];
  uint32_t read = 0, write = 0;
  uint32_t current_read = 0, current_write = 0;
  std::set<BaseConnection *>::const_iterator iterator;

  std::set<BaseConnection *> *pList = server->GetConnections();
  int count = 0;
  while (pList->size() != 2) {
    if (count++ == 10000) {
      HPS_INFO("Size %d", pList->size());
    }
  }
  HPS_INFO("Size %d", pList->size());

  for (iterator = pList->begin(); iterator != pList->end(); ++iterator) {
    BaseConnection *con = *iterator;

    for (int i = 0; i < ITERATIONS_; i++) {
      for (int j = 0; j < SIZE_; j++) {
        values[j] = 0;
      }
      read = 0;
      while (read < BYTES_) {
        con->readData(((uint8_t *) values) + read, sizeof(values) - read, &current_read);
        read += current_read;
        if (current_read == 0) {
          pthread_yield();
        }
      }
    }

    HPS_INFO("Done receiving.. switching to sending");
    current_write = 0;
    write = 0;
    while (current_write < BYTES_) {
      con->writeData((uint8_t *) values + current_write, sizeof(values) - current_write, &write);
      current_write += write;
    }
    HPS_INFO("Done sending..");
  }

  printf("Done rma\n");

  eventLoop->Wait();
  return 0;
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
  connect3();
  exchange3();
  return 0;
}