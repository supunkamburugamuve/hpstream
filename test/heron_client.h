#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_

#include <message.pb.h>
#include <heron_rdma_client.h>

class StMgrClientMgr;

class RDMAStMgrClient : public RDMAClient {
public:
  RDMAStMgrClient(RDMAEventLoopNoneFD* eventLoop, RDMAOptions* _options, RDMAFabric *fabric);
  virtual ~RDMAStMgrClient();

  void Quit();

  void SendTupleStreamMessage(proto::stmgr::TupleMessage* _msg);
  void SendHelloRequest();

protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

private:
  void HandleHelloResponse(void*, proto::stmgr::TupleMessage* _response, NetworkErrorCode);
  void HandleTupleStreamMessage(proto::stmgr::TupleMessage* _message);

  void OnReConnectTimer();
  // Do back pressure
  virtual void StartBackPressureConnectionCb(HeronRDMAConnection* _connection);
  // Relieve back pressure
  virtual void StopBackPressureConnectionCb(HeronRDMAConnection* _connection);

  sp_string other_stmgr_id_;
  bool quit_;
  uint32_t count;

  // Configs to be read
  sp_int32 reconnect_other_streammgrs_interval_sec_;

  // Counters
  sp_int64 ndropped_messages_;
};

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_
