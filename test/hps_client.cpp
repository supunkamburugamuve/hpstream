#include "hps_utils.h"
#include <unistd.h>

Connection *con;
Options options;
struct fi_info *hints;

int connect() {
  int ret;
  Client client(&options, hints);
  client.Connect();
  con = client.GetConnection();
  ret = con->ExchangeClientKeys();
  if (ret) {
    printf("Failed to exchange %d\n", ret);
  } else {
    printf("Exchanged keys\n");
  }
  return ret;
}

int connect3() {
  Client client(&options, hints);
  client.Connect();
  while (con == NULL) {
    sleep(1);
    con = client.GetConnection();
  }
  client.Start();
  return 1;
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
    if (con->RMA(options.rma_op, test_size[0].size)) {
      printf("Failed to RMA \n");
    }
  }
  printf("Done rma\n");
  ret = con->ClientSync();
  if (ret) {
    printf("Failed second sync");
  }
  return ret;
}

int exchange2() {
  int ret = 0;
  int values[10][1000];

  for (int j = 0; j < 10; j++) {
    for (int i = 0; i < 1000; i++) {
      if (j % 2 == 0) {
        values[j][i] = 1000 - i;
      } else {
        values[j][i] = i;
      }
    }
  }

  con->SetupBuffers();
  for (int i = 0; i < 10; i++) {
    con->WriteData((uint8_t *) values[i], sizeof(values[i]));
    con->WriteBuffers();
  }

  printf("Done rma\n");
  return ret;
}

int exchange3() {
  int ret = 0;
  int values[10][1000];

  for (int j = 0; j < 10; j++) {
    for (int i = 0; i < 1000; i++) {
      if (j % 2 == 0) {
        values[j][i] = 1000 - i;
      } else {
        values[j][i] = i;
      }
    }
  }

  for (int i = 0; i < 10; i++) {
    con->WriteData((uint8_t *) values[i], sizeof(values[i]));
  }

  printf("Done rma\n");
  return ret;
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