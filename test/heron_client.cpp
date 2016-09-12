
#include <stdio.h>
#include <iostream>
#include <string>
#include <sptypes.h>
#include <rdma_event_loop.h>
#include <options.h>
#include <heron_rdma_client.h>

#include "heron_client.h"

StMgrClient::StMgrClient(RDMAEventLoopNoneFD* eventLoop, const RDMAOptions* _options,
                         const sp_string& _topology_name, const sp_string& _topology_id,
                         const sp_string& _our_id, const sp_string& _other_id,
                         StMgrClientMgr* _client_manager,
                         heron::common::MetricsMgrSt* _metrics_manager_client)
    : Client(eventLoop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      our_stmgr_id_(_our_id),
      other_stmgr_id_(_other_id),
      quit_(false),
      client_manager_(_client_manager),
      metrics_manager_client_(_metrics_manager_client),
      ndropped_messages_(0) {
  reconnect_other_streammgrs_interval_sec_ =
      config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrClientReconnectIntervalSec();

  InstallResponseHandler(new proto::stmgr::StrMgrHelloRequest(), &StMgrClient::HandleHelloResponse);
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
  if (_status == OK) {
    LOG(INFO) << "Connected to stmgr " << other_stmgr_id_ << " running at "
    << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
    << std::endl;
    if (quit_) {
      Stop();
    } else {
      SendHelloRequest();
    }
  } else {
    LOG(WARNING) << "Could not connect to stmgr " << other_stmgr_id_ << " running at "
    << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
    << " due to: " << _status << std::endl;
    if (quit_) {
      LOG(ERROR) << "Quitting";
      delete this;
      return;
    } else {
      LOG(INFO) << "Retrying again..." << std::endl;
      AddTimer([this]() { this->OnReConnectTimer(); },
               reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
    }
  }
}

void StMgrClient::HandleClose(NetworkErrorCode _code) {
  if (_code == OK) {
    LOG(INFO) << "We closed our server connection with stmgr " << other_stmgr_id_ << " running at "
    << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
    << std::endl;
  } else {
    LOG(INFO) << "Stmgr " << other_stmgr_id_ << " running at " << get_clientoptions().get_host()
    << ":" << get_clientoptions().get_port() << " closed connection with code " << _code
    << std::endl;
  }
  if (quit_) {
    delete this;
  } else {
    LOG(INFO) << "Will try to reconnect again after 1 seconds" << std::endl;
    AddTimer([this]() { this->OnReConnectTimer(); },
             reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
  }
}

void StMgrClient::HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response,
                                      NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOK network code " << _status << " for register response from stmgr "
    << other_stmgr_id_ << " running at " << get_clientoptions().get_host() << ":"
    << get_clientoptions().get_port();
    delete _response;
    Stop();
    return;
  }
  proto::system::StatusCode status = _response->status().status();
  if (status != proto::system::OK) {
    LOG(ERROR) << "NonOK register response " << status << " from stmgr " << other_stmgr_id_
    << " running at " << get_clientoptions().get_host() << ":"
    << get_clientoptions().get_port();
    Stop();
  }
  delete _response;
  if (client_manager_->DidAnnounceBackPressure()) {
    SendStartBackPressureMessage();
  }
}

void StMgrClient::OnReConnectTimer() { Start(); }

void StMgrClient::SendHelloRequest() {
  proto::stmgr::StrMgrHelloRequest* request = new proto::stmgr::StrMgrHelloRequest();
  request->set_topology_name(topology_name_);
  request->set_topology_id(topology_id_);
  request->set_stmgr(our_stmgr_id_);
  SendRequest(request, NULL);
  stmgr_client_metrics_->scope(METRIC_HELLO_MESSAGES_TO_STMGRS)->incr_by(1);
  return;
}

void StMgrClient::SendTupleStreamMessage(proto::stmgr::TupleStreamMessage* _msg) {
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

void StMgrClient::HandleTupleStreamMessage(proto::stmgr::TupleStreamMessage* _message) {
  delete _message;
  LOG(FATAL) << "We should not receive tuple messages in the client" << std::endl;
}

void StMgrClient::StartBackPressureConnectionCb(Connection* _connection) {
}

void StMgrClient::StopBackPressureConnectionCb(Connection* _connection) {
}


