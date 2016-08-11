#ifndef SERVER_H_
#define SERVER_H_

#include <list>

#include "options.h"
#include "utils.h"
#include "connection.h"
#include "event_loop.h"

class Server : public IEventCallback {
public:
  Server(Options *opts, fi_info *hints);
  void Free();
  /**
   * Start the server
   */
  int Init(void);


  bool IsAcceptConnection() {
    return acceptConnections;
  }

  /**
   * Start and run the event loop
   */
  int loop();

  Connection * GetConnection();
  std::list<Connection *> * GetConnections() {
    return &connections;
  }
  int Start();

  int OnEvent(int fid, enum loop_status state);

  int Wait();
private:
  Options *options;
  // hints to be used to obtain fabric information
  struct fi_info *info_hints;
  // hints to be used by passive endpoint
  struct fi_info *info_pep;
  // passive end-point for accepting connections
  struct fid_pep *pep;
  // the event queue to listen on for incoming connections
  struct fid_eq *eq;
  // fid for event queue
  int eq_fid;
  struct fid_domain *domain;
  // event queue attribute
  struct fi_eq_attr eq_attr;
  // the fabric
  struct fid_fabric *fabric;
  // event loop associated with this server
  EventLoop *eventLoop;
  // indicates weather we run the accept connection thread
  bool acceptConnections;

  pthread_t acceptThreadId;
  pthread_t loopThreadId;
  // connections
  Connection *con;
  // list of connections
  std::list<Connection *> connections;

  /**
 * Accept new connections
 */
  int Connect(struct fi_eq_cm_entry *entry);

  /**
   * Disconnect the connection
   * @param con
   * @return
   */
  int Disconnect(Connection *con);
};


#endif /* SSERVER_H_ */