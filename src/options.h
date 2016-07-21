#ifndef HPS_OPTIONS_H_
#define HPS_OPTIONS_H_

#include <stdint-gcc.h>

#include "hps.h"

class Options {
public:
  uint8_t *src_port;
  uint8_t *dst_port;
  uint8_t *src_addr;
  uint8_t *dst_addr;
  uint8_t *fname;
  uint8_t *av_name;
  int transfer_size;
  int options;
  hps_rma_opcodes rma_op;

  // buffer size of a individual buffer, if it is
  // smaller than minimum or greater that maximum supported,
  // it will be adjusted to the minimum
  int buf_size;
  // no of buffers
  int no_buffers;
  /**
   * Computation method, spin, wait or wait-set
   */
  enum hps_comp_method comp_method;

  Options();
  void Free();
private:
};

#endif /* HPS_OPTIONS_H_ */