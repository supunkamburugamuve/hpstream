#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_

#include "network/network_error.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

class StMgrClientMgr;

class StMgrClient : public Client {
public:
  StMgrClient(RDMAEventLoopNoneFD* eventLoop, const RDMAOptions* _options, const sp_string& _topology_name,
              const sp_string& _topology_id, const sp_string& _our_id, const sp_string& _other_id,
              StMgrClientMgr* _client_manager,
              heron::common::MetricsMgrSt* _metrics_manager_client);
  virtual ~StMgrClient();

  void Quit();

  void SendTupleStreamMessage(proto::stmgr::TupleStreamMessage* _msg);

protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

private:
  void HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response, NetworkErrorCode);
  void HandleTupleStreamMessage(proto::stmgr::TupleStreamMessage* _message);

  void OnReConnectTimer();
  void SendHelloRequest();
  // Do back pressure
  virtual void StartBackPressureConnectionCb(Connection* _connection);
  // Relieve back pressure
  virtual void StopBackPressureConnectionCb(Connection* _connection);

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string our_stmgr_id_;
  sp_string other_stmgr_id_;
  bool quit_;

  StMgrClientMgr* client_manager_;
  // Metrics
  heron::common::MetricsMgrSt* metrics_manager_client_;
  heron::common::MultiCountMetric* stmgr_client_metrics_;

  // Configs to be read
  sp_int32 reconnect_other_streammgrs_interval_sec_;

  // Counters
  sp_int64 ndropped_messages_;
};

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_
