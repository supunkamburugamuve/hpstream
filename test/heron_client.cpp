
#include <stdio.h>
#include <iostream>
#include <string>
#include <sptypes.h>
#include <rdma_event_loop.h>
#include <options.h>
#include <heron_rdma_client.h>

#include "heron_client.h"

StMgrClient::StMgrClient(RDMAEventLoopNoneFD* eventLoop, RDMAOptions* _options, RDMAFabric *fabric)
    : Client(_options, fabric, eventLoop),
      quit_(false),
      ndropped_messages_(0) {
  InstallMessageHandler(&StMgrClient::HandleTupleStreamMessage);
}

StMgrClient::~StMgrClient() {
  Stop();
}

void StMgrClient::Quit() {
  quit_ = true;
  Stop();
}

void StMgrClient::HandleConnect(NetworkErrorCode _status) {
}

void StMgrClient::HandleClose(NetworkErrorCode _code) {
  if (quit_) {
    delete this;
  } else {
    LOG(INFO) << "Will try to reconnect again after 1 seconds" << std::endl;
    AddTimer([this]() { this->OnReConnectTimer(); },
             reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
  }
}

void StMgrClient::HandleHelloResponse(void*, proto::stmgr::TupleMessage* _response,
                                      NetworkErrorCode _status) {
}

void StMgrClient::OnReConnectTimer() { Start(); }

void StMgrClient::SendHelloRequest() {
  return;
}

void StMgrClient::SendTupleStreamMessage(proto::stmgr::TupleMessage* _msg) {
  if (IsConnected()) {
    SendMessage(_msg);
  } else {
    if (++ndropped_messages_ % 100 == 0) {
      LOG(INFO) << "Dropping " << ndropped_messages_ << "th tuple message to stmgr "
      << other_stmgr_id_ << " because it is not connected";
    }
    delete _msg;
  }
}

void StMgrClient::HandleTupleStreamMessage(proto::stmgr::TupleMessage* _message) {
  delete _message;
  LOG(FATAL) << "We should not receive tuple messages in the client" << std::endl;
}

void StMgrClient::StartBackPressureConnectionCb(Connection* _connection) {
}

void StMgrClient::StopBackPressureConnectionCb(Connection* _connection) {
}


