#include "hps_utils.h"

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

void rdma_parse_addr_opts(int op, char *optarg, RDMAOptions *opts) {
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
    default:
      /* let getopt handle unknown opts*/
      break;
  }
}
