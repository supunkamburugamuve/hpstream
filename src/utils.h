#ifndef HPS_UTILS_H_
#define HPS_UTILS_H_

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <cstdlib>
#include <cstring>

#include "hps.h"
#include "options.h"

/**
 * Given the options, create node, service, hints and flags
 */
int hps_utils_read_addr_opts(char **node, char **service, struct fi_info *hints,
															uint64_t *flags, Options *opts);
int hps_utils_set_rma_caps(struct fi_info *fi);
int ft_get_cq_fd(Options *opts, struct fid_cq *cq, int *fd);
int hps_utils_get_info(Options *options, struct fi_info *hints, struct fi_info **info);
void hps_utils_cq_set_wait_attr(Options *opts, struct fid_wait *waitset, struct fi_cq_attr *cq_attr);
void hps_utils_cntr_set_wait_attr(Options *opts, struct fid_wait *waitset, struct fi_cntr_attr *cntr_attr);

size_t hps_utils_rx_prefix_size(struct fi_info *fi);
size_t hps_utils_tx_prefix_size(struct fi_info *fi);
uint64_t hps_utils_init_cq_data(struct fi_info *info);
uint64_t hps_utils_caps_to_mr_access(uint64_t caps);
int hps_utils_check_opts(Options *opts, uint64_t flags) ;
int hps_utils_poll_fd(int fd, int timeout);
int hps_utils_cq_readerr(struct fid_cq *cq);
int hps_utils_check_buf(void *buf, int size);


#endif /* end HPS_UTILS */

