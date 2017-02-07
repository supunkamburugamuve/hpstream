
#include <stdio.h>
#include <iostream>
#include <string>
#include <sptypes.h>
#include <rdma_event_loop.h>
#include <options.h>
#include <heron_rdma_client.h>

#include "heron_client.h"

RDMAStMgrClient::RDMAStMgrClient(RDMAEventLoop* eventLoop, RDMAOptions* _options, RDMAFabric *fabric)
    : RDMAClient(_options, fabric, eventLoop),
      quit_(false),
      ndropped_messages_(0) {
  InstallMessageHandler(&RDMAStMgrClient::HandleTupleStreamMessage);
  InstallResponseHandler(new proto::stmgr::TupleMessage(), &RDMAStMgrClient::HandleHelloResponse);
}

RDMAStMgrClient::RDMAStMgrClient(RDMADatagram* eventLoop, RDMAOptions* _options, RDMAFabric *fabric)
    : RDMAClient(_options, fabric, eventLoop),
      quit_(false),
      ndropped_messages_(0) {
  InstallMessageHandler(&RDMAStMgrClient::HandleTupleStreamMessage);
  InstallResponseHandler(new proto::stmgr::TupleMessage(), &RDMAStMgrClient::HandleHelloResponse);
}

RDMAStMgrClient::~RDMAStMgrClient() {
  Stop();
}

void RDMAStMgrClient::Quit() {
  quit_ = true;
  Stop();
}

void RDMAStMgrClient::HandleConnect(NetworkErrorCode _status) {
}

void RDMAStMgrClient::HandleClose(NetworkErrorCode _code) {
  if (quit_) {
    delete this;
  } else {
    LOG(INFO) << "Will try to reconnect again after 1 seconds" << std::endl;
    AddTimer([this]() { this->OnReConnectTimer(); },
             reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
  }
}

void RDMAStMgrClient::HandleHelloResponse(void*, proto::stmgr::TupleMessage* _response,
                                      NetworkErrorCode _status) {
  LOG(INFO) << "Received hello response";
}

void RDMAStMgrClient::OnReConnectTimer() { Start(); }

void RDMAStMgrClient::SendHelloRequest() {
  char *name = new char[100];
  sprintf(name, "Helooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo");
  proto::stmgr::TupleMessage *message = new proto::stmgr::TupleMessage();
  message->set_name(name);
  message->set_id(1);
  message->set_data(name);
  message->set_time(1000.5);
  return;
}

void RDMAStMgrClient::SendTupleStreamMessage(proto::stmgr::TupleMessage* _msg) {
  if (IsConnected()) {
    // LOG(INFO) << "Send message";
    SendMessage(_msg);
  } else {
    if (++ndropped_messages_ % 100 == 0) {
      LOG(INFO) << "Dropping " << ndropped_messages_ << "th tuple message to stmgr "
      << other_stmgr_id_ << " because it is not connected";
    }
    delete _msg;
  }
}

void RDMAStMgrClient::HandleTupleStreamMessage(proto::stmgr::TupleMessage* _message) {
  //LOG(INFO) << _message->id() << " " << _message->data();
  Timer timer;
  count++;
  if (count % 10000 == 0) {
    double t = _message->time();
     printf("count: %d time: %lf\n ", count, (timer.currentTime() - t));
  }
  delete _message;
}

void RDMAStMgrClient::StartBackPressureConnectionCb(HeronRDMAConnection* _connection) {
}

void RDMAStMgrClient::StopBackPressureConnectionCb(HeronRDMAConnection* _connection) {
}


