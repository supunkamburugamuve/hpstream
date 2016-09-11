#include "heron_stmgr_server.h"
#include <iostream>
#include <set>
#include <vector>
#include <heron_rdma_server.h>

// Time spent in back pressure because of local instances connection;
// we initiated this backpressure
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_INIT =
    "__server/__time_spent_back_pressure_initiated";
// Time spent in back pressure because of a component id. The comp id will be
// appended
// to the string below
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_COMPID = "__time_spent_back_pressure_by_compid/";

StMgrServer::StMgrServer(RDMAEventLoopNoneFD* eventLoop, const RDMAOptions *_options,
                         const sp_string& _topology_name, const sp_string& _topology_id,
                         const sp_string& _stmgr_id,
                         const std::vector<sp_string>& _expected_instances, StMgr* _stmgr,
                         heron::common::MetricsMgrSt* _metrics_manager_client)
    : Server(eventLoop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
      expected_instances_(_expected_instances),
      stmgr_(_stmgr) {
  // stmgr related handlers
  InstallRequestHandler(&StMgrServer::HandleStMgrHelloRequest);
  InstallMessageHandler(&StMgrServer::HandleTupleStreamMessage);

  spouts_under_back_pressure_ = false;
}

StMgrServer::~StMgrServer() {
  Stop();
}


sp_string StMgrServer::MakeBackPressureCompIdMetricName(const sp_string& instanceid) {
  return METRIC_TIME_SPENT_BACK_PRESSURE_COMPID + instanceid;
}

void StMgrServer::HandleNewConnection(Connection* _conn) {
  // There is nothing to be done here. Instead we wait
  // for the register/hello
  LOG(INFO) << "Got new connection " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
}

void StMgrServer::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  LOG(INFO) << "Got connection close of " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
}

void StMgrServer::HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                                          proto::stmgr::StrMgrHelloRequest* _request) {
  LOG(INFO) << "Got a hello message from stmgr " << _request->stmgr() << " on connection " << _conn;
  delete _request;
}

void StMgrServer::HandleTupleStreamMessage(Connection* _conn,
                                           proto::stmgr::TupleStreamMessage* _message) {
  ConnectionStreamManagerMap::iterator iter = rstmgrs_.find(_conn);
  if (iter == rstmgrs_.end()) {
    LOG(INFO) << "Recieved Tuple messages from unknown streammanager connection" << std::endl;
  } else {
    if (_message->set().has_data()) {
    } else if (_message->set().has_control()) {
    }
  }
  delete _message;
}

void StMgrServer::StartBackPressureConnectionCb(Connection* _connection) {
  // The connection will notify us when we can stop the back pressure
  _connection->setCausedBackPressure();
}

void StMgrServer::StopBackPressureConnectionCb(Connection* _connection) {
  _connection->unsetCausedBackPressure();
}

void StMgrServer::SendStartBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending start back pressure notification to all other "
            << "stream managers";
  }

void StMgrServer::SendStopBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending stop back pressure notification to all other "
            << "stream managers";
}


