#ifndef CLIENT_H_
#define CLIENT_H_

#include "utils.h"
#include "options.h"
#include "connection.h"

class Client {
public:
  Client(Options *opts, fi_info *hints);
  int Connect(void);
  /**
   * Exchange keys with the peer
   */
  int ExchangeKeys();
  /**
   * Sync
   */
  int sync();
  /**
   * RMA
   */
  ssize_t RMA(enum hps_rma_opcodes op, size_t size);

  size_t Send(char *buf, size_t size);
  int Receive(char *buf, size_t *size);

  int Finalize(void);

  Connection *GetConnection();

  void Free();
private:
  // options for initialization
  Options *options;
  // fabric information obtained
  struct fi_info *info;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // the event queue to for  connection handling
  struct fid_eq *eq;
  // event queue attribute
  struct fi_eq_attr eq_attr;
  // the fabric
  struct fid_fabric *fabric;
  // the connection
  Connection *con;


};

#endif /* SCLIENT_H_ */