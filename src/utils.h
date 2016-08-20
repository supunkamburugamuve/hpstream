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
															uint64_t *flags, RDMAOptions *opts);
int hps_utils_get_cq_fd(RDMAOptions *opts, struct fid_cq *cq, int *fd);
int hps_utils_get_eq_fd(RDMAOptions *opts, struct fid_eq *eq, int *fd);
int hps_utils_get_info(RDMAOptions *options, struct fi_info *hints, struct fi_info **info);

uint64_t hps_utils_caps_to_mr_access(uint64_t caps);
int hps_utils_poll_fd(int fd, int timeout);
int hps_utils_cq_readerr(struct fid_cq *cq);

#define INTEG_SEED 7

int print_short_info(struct fi_info *info);

#endif /* end HPS_UTILS */

