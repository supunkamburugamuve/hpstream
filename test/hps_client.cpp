#include "hps_utils.h"

RDMAConnection *con;
RDMAOptions options;
struct fi_info *hints;
RDMAEventLoop *eventLoop;
RDMAFabric *fabric;
RDMAClient *client;

int connect3() {
  fabric = new RDMAFabric(&options, hints);
  fabric->Init();
  eventLoop = new RDMAEventLoop(fabric->GetFabric());
  client = new RDMAClient(&options, fabric, eventLoop);
  client->Connect();
  con = client->GetConnection();
  eventLoop->Start();
  return 1;
}

int exchange3() {
  int ret = 0;
  int values[10][1000];
  uint32_t read = 0, write = 0;
  read = 0;
  int count = 0;
  uint32_t current_read = 0, current_write = 0;

  for (int j = 0; j < 10; j++) {
    for (int i = 0; i < 1000; i++) {
      if (j % 2 == 0) {
        values[j][i] = j * 1000 + i;
      } else {
        values[j][i] = j * 1000 + i;
      }
    }
  }

  for (int i = 0; i < 10000; i++) {
    current_write = 0;
    write = 0;
    while (current_write < 4000) {
      con->WriteData((uint8_t *) values[i % 10] + current_write, sizeof(values[i]), &write);
      if (write > 0) {
        HPS_INFO("Write amount %d", write);
      }
      current_write += write;
    }
  }

  HPS_INFO("Done sending.. switching to receive");
  while (read < 4000) {
//    if (con->DataAvailableForRead()) {
      con->ReadData(((uint8_t *) (values[0]) + read), sizeof(values[0]) - read, &current_read);
      HPS_INFO("read amount %d", current_read);
      read += current_read;
//    }
  }

  for (int i = 0; i < 1000; i++) {
    printf("%d ", values[0][i]);
  }

  printf("\nDone rma\n");
  return ret;
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