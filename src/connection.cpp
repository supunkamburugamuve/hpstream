#include "connection.h"

#define HPS_POST(post_fn, comp_fn, seq, op_str, ...)        \
  do {                  \
    int timeout_save;            \
    int ret, rc;              \
                    \
    while (1) {              \
      ret = post_fn(__VA_ARGS__);        \
      if (!ret)            \
        break;            \
                    \
      if (ret != -FI_EAGAIN) {        \
        printf("%s %d\n", op_str, ret);      \
        return ret;          \
      }              \
                    \
      timeout_save = timeout;          \
      timeout = 0;            \
      rc = comp_fn(seq);          \
      if (rc && rc != -FI_EAGAIN) {        \
        HPS_ERR("Failed to get %s completion\n", op_str);  \
        return rc;          \
      }              \
      timeout = timeout_save;          \
    }                \
    seq++;                \
  } while (0)

Connection::Connection(Options *opts, struct fi_info *info_hints, struct fi_info *info,
                       struct fid_fabric *fabric, struct fid_domain *domain, struct fid_eq *eq) {
  this->options = opts;
  this->info = info;
  this->info_hints = info_hints;
  this->fabric = fabric;
  this->domain = domain;

  this->txcq = NULL;
  this->rxcq = NULL;
  this->txcntr = NULL;
  this->rxcntr = NULL;

  this->ep = NULL;
  this->alias_ep = NULL;
  this->av = NULL;
  this->mr = NULL;
  this->no_mr = {};

  this->rx_fd = -1;
  this->tx_fd = -1;

  this->tx_buf = NULL;
  this->buf = NULL;
  this->rx_buf = NULL;

  this->buf_size = 0;
  this->tx_size = 0;
  this->rx_size = 0;
  this->recv_buf = NULL;
  this->send_buf = NULL;

  this->remote_cq_data = 0;
  this->waitset = NULL;

  this->cq_attr = {};
  this->cntr_attr = {};
  this->av_attr = {};
  this->tx_ctx = {};
  this->rx_ctx = {};

  this->tx_seq = 0;
  this->rx_seq = 0;
  this->tx_cq_cntr = 0;
  this->rx_cq_cntr = 0;

  this->ft_skip_mr = 0;

  this->cq_attr.wait_obj = FI_WAIT_NONE;
  this->cntr_attr.events = FI_CNTR_EVENTS_COMP;
  this->cntr_attr.wait_obj = FI_WAIT_NONE;

  this->av_attr.type = FI_AV_MAP;
  this->av_attr.count = 1;

  this->remote_fi_addr = FI_ADDR_UNSPEC;
  this->remote = {};

  this->timeout = -1;
}


