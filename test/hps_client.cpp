#include "hps_utils.h"

Connection *con;
Options options;
struct fi_info *hints;

int connect() {
  int ret;
  Client client(&options, hints);
  client.Connect();
  Connection *con = client.GetConnection();
  ret = con->ExchangeClientKeys();
  if (ret) {
    printf("Failed to exchange %d\n", ret);
  } else {
    printf("Exchanged keys\n");
  }
  return ret;
}

int exchange() {
  int ret;
  ret = con->ClientSync();
  if (ret) {
    printf("Failed to sync\n");
  } else {
    printf("synced\n");
  }

  for (int i = 0; i < 10000; i++) {
    options.transfer_size = test_size[0].size;
    if (con->RMA(options.rma_op, options.transfer_size)) {
      printf("Failed to RMA \n");
    }
  }
  printf("Done rma\n");
  ret = con->ClientSync();
  if (ret) {
    printf("Failed second sync");
  }

  ret = con->Finalize();
  if (ret) {
    printf("Failed Finalize");
  }
  return ret;
}

int main(int argc, char **argv) {
  int op;
  int ret = 0;
  options.transfer_size = 100;
  options.rma_op = HPS_RMA_WRITE;
  options.buf_size = 1024 * 1024 * 20;
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

  connect();
  exchange();
  return 0;
}