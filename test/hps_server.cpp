#include "hps_utils.h"

//Connection *con;
Options options;
struct fi_info *hints;
Server *server;

int connect3() {
  int ret = 0;
  server = new Server(&options, hints);
  server->Init();
  //server->Connect();
//  con = server.GetConnection();
  server->Start();
  return ret;
}

int exchange3() {
  int values[1000];
  uint32_t read = 0;
  uint32_t current_read = 0;
  std::list<Connection *>::const_iterator iterator;

  std::list<Connection *> *pList = server->GetConnections();
  int count = 0;
  while (pList->size() != 1) {
    if (count++ == 10000) {
      HPS_INFO("Size %d", pList->size());
    }
  }

  for (iterator = pList->begin(); iterator != pList->end(); ++iterator) {
    Connection *con = *iterator;

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 1000; j++) {
        values[j] = 0;
      }
      read = 0;
      count = 0;
      while (read < 4000 && count < 10) {
        if (con->DataAvailableForRead()) {
          con->ReadData(((uint8_t *) values) + read, sizeof(values) - read, &current_read);
          HPS_INFO("read amount %d", current_read);
          read += current_read;
          count++;
        }
      }
      for (int j = 0; j < 1000; j++) {
        printf("%d ", values[j]);
      }
      printf("\n");
    }

    HPS_INFO("Done receiving.. switching to sending");
    con->WriteData((uint8_t *)values, sizeof(values));
  }

  printf("Done rma\n");

  server->Wait();
  return 0;
}

int main(int argc, char **argv) {
  int op;
  options.rma_op = HPS_RMA_WRITE;
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