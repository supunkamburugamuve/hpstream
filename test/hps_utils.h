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

extern struct test_size_param test_size[];

void rdma_parseinfo(int op, char *optarg, struct fi_info *hints);
void rdma_parse_addr_opts(int op, char *optarg, Options *opts);

