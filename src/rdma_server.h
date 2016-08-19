#ifndef RDMA_SERVER_H_
#define RDMA_SERVER_H_

#include <list>

#include "options.h"
#include "utils.h"
#include "rdma_connection.h"
#include "rdma_event_loop.h"
#include "rdma_fabric.h"

class RDMAServer {
public:
  RDMAServer(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoop *loop);
  void Free();
  /**
   * Start the server
   */
  int Init(void);

  std::list<RDMAConnection *> * GetConnections() {
    return &connections;
  }

  /**
   * Listen for connection events.
   */
  int OnEvent(enum rdma_loop_event event, enum rdma_loop_status state);
private:
  RDMAOptions *options;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // hints to be used by passive endpoint
  struct fi_info *info_pep;
  // passive end-point for accepting connections
  struct fid_pep *pep;
  // the event queue to listen on for incoming connections
  struct fid_eq *eq;
  // the loop callback
  struct rdma_loop_info eq_loop;
  // fid for event queue
  int eq_fid;
  struct fid_domain *domain;
  // event queue attribute
  struct fi_eq_attr eq_attr;
  // the fabric
  struct fid_fabric *fabric;
  // event loop associated with this server
  RDMAEventLoop *eventLoop;
  // list of connections
  std::list<RDMAConnection *> connections;
  // list of connections halfway through fully establishing
  std::list<RDMAConnection *> pending_connections;

  /**
   * Accept a new connection
  */
  int Connect(struct fi_eq_cm_entry *entry);

  /**
   * We are in the second part of connection establishment
   * The connection has being fully established
   */
  int Connected(struct fi_eq_cm_entry *entry);

  /**
   * Disconnect the connection
   * @param con
   * @return
   */
  int Disconnect(RDMAConnection *con);


};


#endif /* SSERVER_H_ */