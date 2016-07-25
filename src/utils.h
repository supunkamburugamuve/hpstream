#ifndef HPS_UTILS_H_
#define HPS_UTILS_H_

#include <string>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include <assert.h>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>
#include <sys/wait.h>

#include "hps.h"
#include "options.h"

#define HPS_CLOSE_FID(fd)					\
	do {							\
		int ret;					\
		if ((fd)) {					\
			ret = fi_close(&(fd)->fid);		\
			if (ret)				\
				HPS_ERR("fi_close (%d) fid %d",	\
					ret, (int) (fd)->fid.fclass);	\
			fd = NULL;				\
		}						\
	} while (0)

/**
 * Given the options, create node, service, hints and flags
 */
int hps_utils_read_addr_opts(char **node, char **service, struct fi_info *hints,
															uint64_t *flags, Options *opts);
int hps_utils_set_rma_caps(struct fi_info *fi);
int hps_utils_get_cq_fd(Options *opts, struct fid_cq *cq, int *fd);
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
#define INTEG_SEED 7
static const char integ_alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int integ_alphabet_length = (sizeof(integ_alphabet)/sizeof(*integ_alphabet)) - 1;

void hps_utils_fill_buf(void *buf, int size);

int print_short_info(struct fi_info *info);

#endif /* end HPS_UTILS */

