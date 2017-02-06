#ifndef RDMA_SERVER_H_
#define RDMA_SERVER_H_

#include <list>
#include <set>

#include "options.h"
#include "utils.h"
#include "rdma_connection.h"
#include "rdma_event_loop.h"
#include "rdma_fabric.h"
#include "rdma_base_connection.h"

class RDMABaseServer {
public:
  RDMABaseServer(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoop *loop);
  RDMABaseServer(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMADatagram *loop);
  virtual ~RDMABaseServer();
  /**
   * Start the server
   */
  int Start_Base(void);

  /**
   * Stop the server
   * @return
   */
  int Stop_Base(void);

  std::set<RDMABaseConnection *> * GetConnections() { return &active_connections_; }

  /**
   * Listen for connection events.
   */
  void OnConnect(enum rdma_loop_status state);

  // Close a connection. This function doesn't return anything.
  // When the connection is attempted to be closed(which can happen
  // at a later time if using thread pool), The HandleConnectionClose
  // will contain a status of how the closing process went.
  void CloseConnection_Base(RDMABaseConnection* connection);

  int AddChannel(uint32_t target_id, char *node, char *service);
protected:
  // Instantiate a new Connection
  virtual RDMABaseConnection* CreateConnection(RDMAChannel* endpoint, RDMAOptions* options,
                                               RDMAEventLoop* ss, ChannelType type) = 0;

  // Called when a new connection is accepted.
  virtual void HandleNewConnection_Base(RDMABaseConnection* newConnection) = 0;

  // Called when a connection is closed.
  // The connection object must not be used by the application after this call.
  virtual void HandleConnectionClose_Base(RDMABaseConnection* connection,
                                          NetworkErrorCode _status) = 0;

  // event loop associated with this server
  RDMAEventLoop *eventLoop_;

  // the datagram loop
  RDMADatagram *datagram_;

  // set of active connections
  std::set<RDMABaseConnection *> active_connections_;

  // list of connections halfway through fully establishing
  std::set<RDMABaseConnection *> pending_connections_;

  RDMAOptions *options;

  RDMAFabric *rdmaFabric;
private:
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
   * Start accepting connections
   */
  int StartAcceptingConnections();
};

#endif /* SSERVER_H_ */