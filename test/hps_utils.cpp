#include "hps_utils.h"

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
