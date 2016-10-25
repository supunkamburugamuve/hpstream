#ifndef CLIENT_H_
#define CLIENT_H_

#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <glog/logging.h>

#include "rdma_base_connecion.h"
#include "connection.h"
#include "rdma_client.h"
#include "ridgen.h"

/*
 * Client class definition
 * Given a host/port, the client tries to connect to the host port.
 * It calls various virtual methods when events happen. The events are
 * HandleConnect:- When the connect process completes, this method is invoked.
 *                 Applications can send a greeting message, etc.
 * HandleClose:- We call this function when the underlying connection closes.
 *               A connection can close either because the user explictly
 *               did a close using the Stop API or because of a read/write
 *               error encountered by the underlying connection.
 *               Derived classes can cleanup stuff in this method
 * HandleResponse:- Whenever a response from the server arrives, this method
 *                  is called. Derived classes can parse the response and
 *                  use the response in this method.
 * Derived classes can use the SendRequest method to send a request to the
 * server. They can use the Stop to explicitly close a connection.
 */
class Client : public RDMABaseClient {
public:
  // Constructor/Destructor
  // Note that constructor doesn't do much beyond initializing some members.
  // Users must explicitly invoke the Start method to be able to send requests
  // and receive responses.
  Client(RDMAOptions *opts, RDMAFabric *rdmaFabric, RDMAEventLoopNoneFD *loop);
  virtual ~Client();

  // This starts the connect opereation.
  // A return of this function doesnt mean that the client is ready to go.
  // It just means that the connect operation is proceeding and we will
  // be informed with the HandleConnect method how things went.
  void Start();

  // This one closes the underlying connection. No new responses will
  // be delivered to the client after this call. The set of existing
  // requests may not be sent by the client depending on whether they
  // have already been sent out of wire or not. You might receive
  // error notifications via the HandleSentRequest.
  // A return from this doesn't mean that the underlying sockets have been
  // closed. Merely that the process has begun. When the actual close
  // happens, HandleClose will be called.
  void Stop();

  // Send a request to the server with a certain timeout
  // This function doesnt return anything. After this function returns,
  // does not mean that the request actually sent out, merely that the request
  // was successfully queued to be sent out.
  // Actual send occurs when the socket becomes readable and all prev
  // requests are sent. If the packet cannot be sent
  // out or the request is not retired by the client within the timeout
  // period, the HandleResponse is called with the appropriate status.
  // The _request is now owned by the Client class. The ctx is
  // a user owned piece of context that is not interpreted by the
  // client which is passed on to the HandleResponse
  // A negative value of the msecs means no timeout.
  void SendRequest(google::protobuf::Message* _request, void* ctx, int64_t msecs);

  // Convinience method of the above function with no timeout
  void SendRequest(google::protobuf::Message* _request, void* ctx);

  // This interface is used if you want to communicate with the other end
  // on a non-request-response based communication.
  void SendMessage(google::protobuf::Message* _message);

  // Add a timer to be invoked after msecs microseconds. Returns the timer id.
  sp_int64 AddTimer(VCallback<> cb, int64_t msecs);
  // Removes a timer with timer_id
  sp_int32 RemoveTimer(int64_t timer_id);

  // For server type request handling
  void SendResponse(REQID _id, const google::protobuf::Message& response);

  // Tells if we are connected
  //inline bool IsConnected() const { return state_ == CONNECTED; }

  // Register a handler for a particular response type
  template <typename S, typename T, typename M>
  void InstallResponseHandler(S* _request,
                              void (T::*method)(void* _ctx, M*, NetworkErrorCode status)) {
    google::protobuf::Message* m = new M();
    T* t = static_cast<T*>(this);
    responseHandlers[m->GetTypeName()] = std::bind(&Client::dispatchResponse<T, M>, this, t, method,
                                                   std::placeholders::_1, std::placeholders::_2);
    requestResponseMap_[_request->GetTypeName()] = m->GetTypeName();
    delete m;
    delete _request;
  }

  // Register a handler for a particular request type
  template <typename T, typename M>
  void InstallRequestHandler(void (T::*method)(REQID id, M*)) {
    google::protobuf::Message* m = new M();
    T* t = static_cast<T*>(this);
    requestHandlers[m->GetTypeName()] =
        std::bind(&Client::dispatchRequest<T, M>, this, t, method, std::placeholders::_1);
    delete m;
  }

  // Register a handler for a particular message type
  template <typename T, typename M>
  void InstallMessageHandler(void (T::*method)(M* _message)) {
    google::protobuf::Message* m = new M();
    T* t = static_cast<T*>(this);
    messageHandlers[m->GetTypeName()] =
        std::bind(&Client::dispatchMessage<T, M>, this, t, method, std::placeholders::_1);
    delete m;
  }

  int64_t getOutstandingPackets() const {
    if (conn_) {
      return (reinterpret_cast<Connection*>(conn_))->getOutstandingPackets();
    } else {
      return 0;
    }
  }

  int64_t getOutstandingBytes() const {
    if (conn_) {
      return (reinterpret_cast<Connection*>(conn_))->getOutstandingBytes();
    } else {
      return 0;
    }
  }

  // Return the underlying EventLoop.
  RDMAEventLoopNoneFD* getEventLoop() { return eventLoop_; }

protected:
  // Derived class should implement this method to handle Connection
  // establishment. a status of OK implies that the Client was
  // successful in connecting to hte client. Requests can now be sent to
  // the server. Any other status implies that the connect failed.
  virtual void HandleConnect(NetworkErrorCode status) = 0;

  // When the underlying socket is closed(either because of an explicit
  // Stop done by derived class or because a read/write failed and
  // the connection closed automatically on us), this method is
  // called. A status of OK means that this was a user initiated
  // Close that successfully went through. A status value of
  // READ_ERROR implies that there was problem reading in the
  // connection and thats why the connection was closed internally.
  // A status value of WRITE_ERROR implies that there was a problem writing
  // in the connection. Derived classes can do any cleanups in this method.
  virtual void HandleClose(NetworkErrorCode status) = 0;

  // friend classes that can access the protected functions
  friend void CallHandleSentRequestAndDelete(Client*, google::protobuf::Message*, void* ctx,
                                             NetworkErrorCode);
  // Backpressure handler
  virtual void StartBackPressureConnectionCb(Connection* connection);
  // Backpressure Reliever
  virtual void StopBackPressureConnectionCb(Connection* _connection);

private:
  //! Imlement methods of BaseClient
  virtual BaseConnection* CreateConnection(RDMAConnection* endpoint, RDMAOptions* options,
                                           RDMAEventLoopNoneFD* ss);
  virtual void HandleConnect_Base(NetworkErrorCode status);
  virtual void HandleClose_Base(NetworkErrorCode status);

  //! Handle most of the init stuff
  void Init();

  void InternalSendRequest(google::protobuf::Message* _request, void* _ctx, int64_t _msecs);
  void InternalSendMessage(google::protobuf::Message* _request);
  void InternalSendResponse(RDMAOutgoingPacket* _packet);

  // Internal method to be called by the Connection class
  // when a new packet arrives
  void OnNewPacket(RDMAIncomingPacket* packet);

  // Internal method to be called by the EventLoop class
  // when a packet timer expires
  void OnPacketTimer(REQID _id, RDMAEventLoopNoneFD::Status status);

  template <typename T, typename M>
  void dispatchResponse(T* _t, void (T::*method)(void* _ctx, M*, NetworkErrorCode),
                        RDMAIncomingPacket* _ipkt, NetworkErrorCode _code) {
    void* ctx = NULL;
    M* m = NULL;
    NetworkErrorCode status = _code;
    if (status == OK && _ipkt) {
      REQID rid;
      CHECK(_ipkt->UnPackREQID(&rid) == 0) << "REQID unpacking failed";
      if (context_map_.find(rid) != context_map_.end()) {
        // indeed
        ctx = context_map_[rid].second;
        m = new M();
        context_map_.erase(rid);
        _ipkt->UnPackProtocolBuffer(m);
      } else {
        // This is either some unknown message type or the response of an
        // already timed out request
        std::cerr << "Dropping an incoming packet because either the message type is unknown "
                  << " or it was a response for an already timed out request" << std::endl;
        return;
      }
    }

    std::function<void()> cb = std::bind(method, _t, ctx, m, status);

    cb();
  }

  template <typename T, typename M>
  void dispatchRequest(T* _t, void (T::*method)(REQID id, M*), RDMAIncomingPacket* _ipkt) {
    REQID rid;
    CHECK(_ipkt->UnPackREQID(&rid) == 0) << "REQID unpacking failed";
    M* m = new M();
    if (_ipkt->UnPackProtocolBuffer(m) != 0) {
      // We could not decode the pb properly
      std::cerr << "Could not decode protocol buffer of type " << m->GetTypeName();
      delete m;
      return;
    }
    CHECK(m->IsInitialized());

    std::function<void()> cb = std::bind(method, _t, rid, m);

    cb();
  }

  template <typename T, typename M>
  void dispatchMessage(T* _t, void (T::*method)(M*), RDMAIncomingPacket* _ipkt) {
    M* m = new M();
    if (_ipkt->UnPackProtocolBuffer(m) != 0) {
      // We could not decode the pb properly
      std::cerr << "Could not decode protocol buffer of type " << m->GetTypeName();
      delete m;
      return;
    }
    CHECK(m->IsInitialized());

    std::function<void()> cb = std::bind(method, _t, m);

    cb();
  }

  //! Map from reqid to the response/context pair of the request
  std::unordered_map<REQID, std::pair<string, void*>, REQID_Hash> context_map_;

  typedef std::function<void(RDMAIncomingPacket*)> handler;
  std::unordered_map<std::string, handler> requestHandlers;
  std::unordered_map<std::string, handler> messageHandlers;
  typedef std::function<void(RDMAIncomingPacket*, NetworkErrorCode)> res_handler;
  std::unordered_map<std::string, res_handler> responseHandlers;
  std::unordered_map<std::string, std::string> requestResponseMap_;

  // REQID generator
  REQID_Generator* message_rid_gen_;
};

#endif  // CLIENT_H_
