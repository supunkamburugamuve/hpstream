#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cstring>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>
#include <poll.h>

#include "utils.h"
#include "options.h"

char default_port[8] = "9228";

int hps_utils_set_rma_caps(struct fi_info *fi) {
  fi->caps |= FI_REMOTE_READ;
  if (fi->mode & FI_LOCAL_MR)
    fi->caps |= FI_READ;

  fi->caps |= FI_REMOTE_WRITE;
  if (fi->mode & FI_LOCAL_MR)
    fi->caps |= FI_WRITE;
  return 0;
}

int hps_utils_get_cq_fd(Options *opts, struct fid_cq *cq, int *fd) {
  int ret = FI_SUCCESS;

  if (cq && opts->comp_method == HPS_COMP_WAIT_FD) {
    ret = fi_control(&cq->fid, FI_GETWAIT, fd);
    if (ret) {
      HPS_ERR("fi_control(FI_GETWAIT) %d", ret);
    }
  }

  return ret;
}

size_t hps_utils_tx_prefix_size(struct fi_info *fi) {
  return (fi->tx_attr->mode & FI_MSG_PREFIX) ?
         fi->ep_attr->msg_prefix_size : 0;
}

size_t hps_utils_rx_prefix_size(struct fi_info *fi) {
  return (fi->rx_attr->mode & FI_MSG_PREFIX) ?
         fi->ep_attr->msg_prefix_size : 0;
}

uint64_t hps_utils_init_cq_data(struct fi_info *info) {
  if (info->domain_attr->cq_data_size >= sizeof(uint64_t)) {
    return 0x0123456789abcdefULL;
  } else {
    return 0x0123456789abcdef &
           ((0x1ULL << (info->domain_attr->cq_data_size * 8)) - 1);
  }
}

void hps_utils_cq_set_wait_attr(Options *opts, struct fid_wait *waitset, struct fi_cq_attr *cq_attr) {
  switch (opts->comp_method) {
    case HPS_COMP_SREAD:
      cq_attr->wait_obj = FI_WAIT_UNSPEC;
      cq_attr->wait_cond = FI_CQ_COND_NONE;
      break;
    case HPS_COMP_WAITSET:
      cq_attr->wait_obj = FI_WAIT_SET;
      cq_attr->wait_cond = FI_CQ_COND_NONE;
      cq_attr->wait_set = waitset;
      break;
    case HPS_COMP_WAIT_FD:
      cq_attr->wait_obj = FI_WAIT_FD;
      cq_attr->wait_cond = FI_CQ_COND_NONE;
      break;
    default:
      cq_attr->wait_obj = FI_WAIT_NONE;
      break;
  }
}

void hps_utils_cntr_set_wait_attr(Options *opts, struct fid_wait *waitset, struct fi_cntr_attr *cntr_attr) {
  switch (opts->comp_method) {
    case HPS_COMP_SREAD:
      cntr_attr->wait_obj = FI_WAIT_UNSPEC;
      break;
    case HPS_COMP_WAITSET:
      cntr_attr->wait_obj = FI_WAIT_SET;
      break;
    case HPS_COMP_WAIT_FD:
      cntr_attr->wait_obj = FI_WAIT_FD;
      break;
    default:
      cntr_attr->wait_obj = FI_WAIT_NONE;
      break;
  }
}

static int hps_utils_dupaddr(void **dst_addr, size_t *dst_addrlen,
                              void *src_addr, size_t src_addrlen) {
  *dst_addr = malloc(src_addrlen);
  *dst_addrlen = src_addrlen;
  memcpy(*dst_addr, src_addr, src_addrlen);
  return 0;
}

/**
 * Get the address according to the format supported by the device
 */
static int hps_utils_getaddr(char *node, char *service,
                             struct fi_info *hints, uint64_t flags) {
  int ret;
  struct fi_info *fi;

  if (!node && !service) {
    if (flags & FI_SOURCE) {
      hints->src_addr = NULL;
      hints->src_addrlen = 0;
    } else {
      hints->dest_addr = NULL;
      hints->dest_addrlen = 0;
    }
    return 0;
  }

  printf("Get info with options node=%s service=%s flags=%d\n", node, service, (int)flags);

  ret = fi_getinfo(HPS_FIVERSION, node, service, flags, hints, &fi);
  if (ret) {
    return ret;
  }
  hints->addr_format = fi->addr_format;

  if (flags & FI_SOURCE) {
    ret = hps_utils_dupaddr(&hints->src_addr, &hints->src_addrlen,
                             fi->src_addr, fi->src_addrlen);
  } else {
    ret = hps_utils_dupaddr(&hints->dest_addr, &hints->dest_addrlen,
                             fi->dest_addr, fi->dest_addrlen);
  }

  fi_freeinfo(fi);
  return ret;
}

int hps_utils_read_addr_opts(char **node, char **service, struct fi_info *hints,
                              uint64_t *flags, Options *opts) {
  int ret;

  if (opts->dst_addr) {
    if (!opts->dst_port) {
      opts->dst_port = default_port;
    }

    ret = hps_utils_getaddr(opts->src_addr, opts->src_port, hints, FI_SOURCE);
    if (ret) {
      return ret;
    }
    *node = opts->dst_addr;
    *service = opts->dst_port;
  } else {
    if (!opts->src_port) {
      opts->src_port = default_port;
    }
    *node = opts->src_addr;
    *service = opts->src_port;
    *flags = FI_SOURCE;
  }

  return 0;
}

static int print_short_info(struct fi_info *info) {
  for (struct fi_info *cur = info; cur; cur = cur->next) {
    printf("provider: %s\n", cur->fabric_attr->prov_name);
    printf("    fabric: %s\n", cur->fabric_attr->name),
        printf("    domain: %s\n", cur->domain_attr->name),
        printf("    version: %d.%d\n", FI_MAJOR(cur->fabric_attr->prov_version),
               FI_MINOR(cur->fabric_attr->prov_version));
    if (!1) {
      printf("    type: %s\n", fi_tostr(&cur->ep_attr->type, FI_TYPE_EP_TYPE));
      printf("    protocol: %s\n", fi_tostr(&cur->ep_attr->protocol, FI_TYPE_PROTOCOL));
    }
  }
  return EXIT_SUCCESS;
}


int hps_utils_get_info(Options *options, struct fi_info *hints, struct fi_info **info) {
  char *fi_str;
  char *node, *service;
  uint64_t flags = 0;
  // read the parameters from the options
  hps_utils_read_addr_opts(&node, &service, hints, &flags, options);

  // default to RDM
  if (!hints->ep_attr->type) {
    hints->ep_attr->type = FI_EP_RDM;
  }

  // now lets retrieve the available network services
  // according to hints
  printf("node=%s service=%s flags=%d\n", node, service, (int)flags);
  int ret = fi_getinfo(HPS_FIVERSION, node, service, flags, hints, info);
  if (ret) {
    HPS_ERR("Fi_info failed %d", ret);
    return 1;
  }

  if (*info) {
    fi_info *next = *info;
    while (next) {
      fi_fabric_attr *attr = next->fabric_attr;
      printf("fabric attr name=%s prov_name=%s\n", attr->name, attr->prov_name);
      fi_str = fi_tostr(next, FI_TYPE_INFO);
      std::cout << "FI" << fi_str << std::endl;
      print_short_info(next);
      next = next->next;
    }
  } else {
    HPS_ERR("No information returned");
    return 1;
  }

  return 0;
}

uint64_t hps_utils_caps_to_mr_access(uint64_t caps) {
  uint64_t mr_access = 0;

  if (caps & (FI_MSG | FI_TAGGED)) {
    if (caps & HPS_MSG_MR_ACCESS)
      mr_access |= caps & HPS_MSG_MR_ACCESS;
    else
      mr_access |= HPS_MSG_MR_ACCESS;
  }

  if (caps & (FI_RMA | FI_ATOMIC)) {
    if (caps & HPS_RMA_MR_ACCESS)
      mr_access |= caps & HPS_RMA_MR_ACCESS;
    else
      mr_access |= HPS_RMA_MR_ACCESS;
  }

  return mr_access;
}

int hps_utils_check_opts(Options *opts, uint64_t flags) {
  return (opts->options & flags) == flags;
}

int hps_utils_poll_fd(int fd, int timeout) {
  struct pollfd fds;
  int ret;

  fds.fd = fd;
  fds.events = POLLIN;
  ret = poll(&fds, 1, timeout);
  if (ret == -1) {
    HPS_ERR("poll %d", -errno);
    ret = -errno;
  } else if (!ret) {
    ret = -FI_EAGAIN;
  } else {
    ret = 0;
  }
  return ret;
}

int hps_utils_cq_readerr(struct fid_cq *cq){
  struct fi_cq_err_entry cq_err;
  ssize_t ret;

  ret = fi_cq_readerr(cq, &cq_err, 0);
  if (ret < 0) {
    HPS_ERR("fi_cq_readerr %ld", ret);
  } else {
    printf("%s\n", fi_cq_strerror(cq, cq_err.prov_errno,
                                  cq_err.err_data, NULL, 0));
    ret = -cq_err.err;
  }
  return (int) ret;
}

void hps_utils_fill_buf(void *buf, int size) {
  char *msg_buf;
  int msg_index;
  static unsigned int iter = 0;
  int i;

  msg_index = ((iter++)*INTEG_SEED) % integ_alphabet_length;
  msg_buf = (char *)buf;
  for (i = 0; i < size; i++) {
    msg_buf[i] = integ_alphabet[msg_index++];
    if (msg_index >= integ_alphabet_length)
      msg_index = 0;
  }
}

int hps_utils_check_buf(void *buf, int size) {
  char *recv_data;
  char c;
  static unsigned int iter = 0;
  int msg_index;
  int i;

  msg_index = ((iter++)*INTEG_SEED) % integ_alphabet_length;
  recv_data = (char *)buf;

  for (i = 0; i < size; i++) {
    c = integ_alphabet[msg_index++];
    if (msg_index >= integ_alphabet_length)
      msg_index = 0;
    if (c != recv_data[i])
      break;
  }
  if (i != size) {
    printf("Error at iteration=%d size=%d byte=%d\n",
           iter, size, i);
    return 1;
  }

  return 0;
}