#include <iostream>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>

#include "client.h"
#include "server.h"

#define ADDR_OPTS "b:p:s:a:r:"
#define INFO_OPTS "n:f:e:"

struct test_size_param {
  uint32_t size;
  int enable_flags;
};

#define FT_DEFAULT_SIZE		(1 << 0)

struct test_size_param test_size[] = {
    { 1 <<  1, 0 }, { (1 <<  1) + (1 <<  0), 0 },
    { 1 <<  2, 0 }, { (1 <<  2) + (1 <<  1), 0 },
    { 1 <<  3, 0 }, { (1 <<  3) + (1 <<  2), 0 },
    { 1 <<  4, 0 }, { (1 <<  4) + (1 <<  3), 0 },
    { 1 <<  5, 0 }, { (1 <<  5) + (1 <<  4), 0 },
    { 1 <<  6, FT_DEFAULT_SIZE }, { (1 <<  6) + (1 <<  5), 0 },
    { 1 <<  7, 0 }, { (1 <<  7) + (1 <<  6), 0 },
    { 1 <<  8, FT_DEFAULT_SIZE }, { (1 <<  8) + (1 <<  7), 0 },
    { 1 <<  9, 0 }, { (1 <<  9) + (1 <<  8), 0 },
    { 1 << 10, FT_DEFAULT_SIZE }, { (1 << 10) + (1 <<  9), 0 },
    { 1 << 11, 0 }, { (1 << 11) + (1 << 10), 0 },
    { 1 << 12, FT_DEFAULT_SIZE }, { (1 << 12) + (1 << 11), 0 },
    { 1 << 13, 0 }, { (1 << 13) + (1 << 12), 0 },
    { 1 << 14, 0 }, { (1 << 14) + (1 << 13), 0 },
    { 1 << 15, 0 }, { (1 << 15) + (1 << 14), 0 },
    { 1 << 16, FT_DEFAULT_SIZE }, { (1 << 16) + (1 << 15), 0 },
    { 1 << 17, 0 }, { (1 << 17) + (1 << 16), 0 },
    { 1 << 18, 0 }, { (1 << 18) + (1 << 17), 0 },
    { 1 << 19, 0 }, { (1 << 19) + (1 << 18), 0 },
    { 1 << 20, FT_DEFAULT_SIZE }, { (1 << 20) + (1 << 19), 0 },
    { 1 << 21, 0 }, { (1 << 21) + (1 << 20), 0 },
    { 1 << 22, 0 }, { (1 << 22) + (1 << 21), 0 },
    { 1 << 23, 0 },
};

void rdma_parseinfo(int op, char *optarg, struct fi_info *hints) {
  switch (op) {
    case 'n':
      if (!hints->domain_attr) {
        hints->domain_attr = (struct fi_domain_attr	*)malloc(sizeof *(hints->domain_attr));
        if (!hints->domain_attr) {
          exit(EXIT_FAILURE);
        }
      }
      hints->domain_attr->name = strdup(optarg);
      break;
    case 'f':
      if (!hints->fabric_attr) {
        hints->fabric_attr = (struct fi_fabric_attr	*	)malloc(sizeof *(hints->fabric_attr));
        if (!hints->fabric_attr) {
          exit(EXIT_FAILURE);
        }
      }
      printf("prov_name: %s\n", optarg);
      hints->fabric_attr->prov_name = strdup(optarg);
      break;
    case 'e':
      if (!strncasecmp("msg", optarg, 3)) {
        hints->ep_attr->type = FI_EP_MSG;
      }
      if (!strncasecmp("rdm", optarg, 3)) {
        hints->ep_attr->type = FI_EP_RDM;
      }
      if (!strncasecmp("dgram", optarg, 5)) {
        hints->ep_attr->type = FI_EP_DGRAM;
      }
      break;
    default:
      break;
  }
}

void rdma_parse_addr_opts(int op, char *optarg, Options *opts) {
  switch (op) {
    case 's':
      printf("source addr: %s\n", optarg);
      opts->src_addr = optarg;
      break;
    case 'b':
      printf("source port: %s\n", optarg);
      opts->src_port = optarg;
      break;
    case 'p':
      printf("dst port: %s\n", optarg);
      opts->dst_port = optarg;
      break;
    case 'r':
      printf("fname: %s\n", optarg);
      opts->fname = strdup(optarg);
      break;
    default:
      /* let getopt handle unknown opts*/
      break;
  }
}

int rma(int argc, char **argv) {
  int op;
  int ret = 0;
  Options options;
  options.transfer_size = 100;
  options.rma_op = HPS_RMA_WRITE;
  options.buf_size = 1024 * 1024 * 20;
  options.no_buffers = 10;
  struct fi_info *hints = fi_allocinfo();
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

  if (options.dst_addr) {
    Client client(&options, hints);
    client.Connect();
    Connection *con = client.GetConnection();
    ret = con->ExchangeClientKeys();
    if (ret) {
      printf("Failed to exchange %d\n", ret);
    } else {
      printf("Exchanged keys\n");
    }

    ret = con->sync();
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
    ret = con->sync();
    if (ret) {
      printf("Failed second sync");
    }
    ret = con->Finalize();
    if (ret) {
      printf("Failed Finalize");
    }
  } else {
    Server server(&options, hints);
    server.Start();
    server.Connect();
    Connection *con = server.con;
    ret = con->ExchangeServerKeys();
    if (ret) {
      printf("Failed to exchange %d\n", ret);
    } else {
      printf("Exchanged keys\n");
    }
    ret = con->sync();
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
    ret = con->sync();
    if (ret) {
      printf("Failed second sync");
    }
    ret = con->Finalize();
    if (ret) {
      printf("Failed Finalize");
    }
  }

  return 0;
}

int main(int argc, char **argv) {
  rma(argc, argv);
}