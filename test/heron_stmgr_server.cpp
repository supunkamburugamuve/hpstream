#include "heron_stmgr_server.h"
#include <iostream>
#include <set>
#include <vector>
#include <heron_rdma_server.h>

const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_COMPID = "__time_spent_back_pressure_by_compid/";

StMgrServer::StMgrServer(RDMAEventLoopNoneFD* eventLoop, RDMAOptions *_options, RDMAFabric *fabric)
    : Server(fabric, eventLoop, _options) {
  // stmgr related handlers
  InstallMessageHandler(&StMgrServer::HandleTupleStreamMessage);
  LOG(INFO) << "Init server";
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

void StMgrServer::HandleTupleStreamMessage(Connection* _conn,
                                           proto::stmgr::TupleMessage* _message) {
  LOG(INFO) << _message->id() << " " << _message->data();

  char *name = new char[100];
  sprintf(name, "Hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii");
  proto::stmgr::TupleMessage *message = new proto::stmgr::TupleMessage();
  message->set_name(name);
  message->set_id(10);
  message->set_data(name);

  SendMessage(_conn, (*message));
  delete message;
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


