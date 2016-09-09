#include "hps_utils.h"
#include <ctime>

struct timespec start, end_t;
RDMAConnection *con;
RDMAOptions options;
struct fi_info *hints;
RDMAEventLoopNoneFD *eventLoop;
RDMAFabric *fabric;
RDMABaseClient *client;

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
  eventLoop = new RDMAEventLoopNoneFD(fabric->GetFabric());
  client = new RDMABaseClient(&options, fabric, eventLoop);
  client->Start_base();
  eventLoop->Start();
  while (!client->IsConnected());
  con = client->GetConnection();
  return 1;
}

int exchange3() {
  int ret = 0;
  int64_t elapsed = 0;
  double rate = 0;
  int values[10][SIZE_];
  uint32_t read = 0, write = 0;
  read = 0;
  uint32_t current_read = 0, current_write = 0;

  for (int j = 0; j < 10; j++) {
    for (int i = 0; i < SIZE_; i++) {
      values[j][i] = j * SIZE_ + i;
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &start);
  elapsed = get_elapsed(&end_t, &start);

  for (int i = 0; i < ITERATIONS_; i++) {
    current_write = 0;
    write = 0;
    while (current_write < BYTES_) {
      con->WriteData((uint8_t *) values[i % 10] + current_write, sizeof(values[i]) - current_write, &write);
      if (write > 0 && i % 100 == 0) {
        //HPS_INFO("Write amount %d %d", write, i);
      }
      if (write == 0) {
        pthread_yield();
      }
      current_write += write;
    }
    if (i % 1000 == 0) {
      // HPS_INFO("Completed %d", i);
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &end_t);
  rate = 1000000.0 * 4000.0 /((1024 * 1024)* (elapsed / (1000 * 1000)));
  HPS_INFO("Message rate: time=%ld s and throughput=%lf", elapsed / 1000000, rate);

  HPS_INFO("Done sending.. switching to receive");
  while (read < BYTES_) {
    con->ReadData(((uint8_t *) values[0]) + read, BYTES_ - read, &current_read);
    read += current_read;
  }

  for (int i = 0; i < SIZE_; i++) {
    printf("%d ", values[0][i]);
  }

  printf("\nDone rma\n");
  return ret;
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