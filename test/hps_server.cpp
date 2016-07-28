#include "hps_utils.h"

Connection *con;
Options options;
struct fi_info *hints;

int connect() {
  int ret;
  Server server(&options, hints);
  server.Start();
  server.Connect();
  con = server.con;
  ret = con->ExchangeServerKeys();
  if (ret) {
    printf("Failed to exchange %d\n", ret);
  } else {
    printf("Exchanged keys\n");
  }
  return ret;
}

int exchange() {
  int ret;
  ret = con->ServerSync();
  if (ret) {
    printf("Failed to sync\n");
  } else {
    printf("synced\n");
  }
  for (int i = 0; i < 10000; i++) {
    if (con->RMA(options.rma_op, test_size[0].size)) {
      printf("Failed to RMA \n");
    }
  }
  printf("Done rma\n");
  ret = con->ServerSync();
  if (ret) {
    printf("Failed second sync");
  }
  return 0;
}

int exchange2() {
  int ret;
  int values[1000];

  //ret = con->ServerSync();
//  if (ret) {
//    printf("Failed to sync\n");
//  } else {
//    printf("synced\n");
//  }
  // this should be moved to connection
  con->SetupBuffers();
  uint32_t read = 0;
  uint32_t current_read = 0;
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 1000; j++) {
      values[j] = 0;
    }
    read = 0;
    int count = 0;
    while (read < 1000 && count < 100) {
      if (!con->DataAvailableForRead()) {
        con->Receive();
      }
      con->ReadData((uint8_t *) values + read, sizeof(values) - read, &current_read);
      HPS_INFO("read amount %d", current_read);
      read += current_read;
      count++;
    }
    for (int j = 0; j < 1000; j++) {
      printf("%d ", values[j]);
    }
    printf("\n");
  }

  printf("Done rma\n");
  return 0;
}

int main(int argc, char **argv) {
  int op;
  int ret = 0;
  options.rma_op = HPS_RMA_WRITE;
  options.buf_size = 1024 * 1024 * 40;
  options.no_buffers = 4;
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
  connect();
  exchange2();
  return 0;
}