#include <tiff.h>

#ifndef RDMA_BASE_CONNECTION_H_

/*
 * An abstract base class to represent a network connection between 2 endpoints.
 * Provides support for non-blocking reads and writes.
 */
class RDMABaseConnection {
public:
  // The state of the connection
  enum State {
    // This is the state of the connection when its created.
        INIT = 0,
    // Connected. Read/Write going fine
        CONNECTED,
    // socket disconnected. No read writes happening
        DISCONNECTED,
    // socket is marked to be disconnected.
        TO_BE_DISCONNECTED,
  };

  // Whether a read/write would block?
  enum ReadWriteState { NOTREGISTERED, READY, NOTREADY, ERROR };

  RDMABaseConnection();
  virtual ~RDMABaseConnection();

  /**
   * Start the connection. Should only be called when in INIT state.
   * Upon success, moves the state from INIT to CONNECTED.
   * Return 0 -> success (ready to send/receive packets)
   * Return -1 -> failure.
   *
   * TODO (vikasr): return the reason for failure (error codes)
   */
  int start();

  /**
   * Close the connection. This will disable the connection from receiving and sending the
   * packets. This will also close the underlying socket structures and invoke a close callback
   * if any was registered. Moves the state to DISCONNECTED.
   */
  void closeConnection();

  /**
   * Register a callback to be called upon connection close
   */
  void registerForClose(VCallback<NetworkErrorCode> cb);

  char* getIPAddress();

  int getPort();

protected:
  /**
   * Writes data out on this connection
   * This method is called by the base class if the connection fd is
   * registeredForWrite and it is writable.
   *
   * A return value of:
   *  - 0 indicates the data is successfully written.
   *  - negative indicates some error.
   */
  virtual uint32 writeIntoEndPoint(sp_int32 _fd) = 0;

  /**
   * A way for base class to know if the derived class still has data to be written.
   * This is called after WriteIntoEndPoint.
   * If true, base class will registerForWrite until this method returns false.
   */
  virtual bool stillHaveDataToWrite() = 0;

  /**
   * Called after WriteIntoEndPoint is successful.
   * Usually meant for cleaning up packets that have been sent.
   */
  virtual void handleDataWritten() = 0;

  /**
   * Called by the base class when the connection fd is readable.
   * The derived classes read in the data coming into the connection.
   *
  * A return value of:
   *  - 0 indicates the data is successfully read.
   *  - negative indicates some error.
   */
  virtual uint32 readFromEndPoint() = 0;

  /**
   * Called after ReadFromEndPoint is successful.
   * Meant for processing packets that have been received.
   */
  virtual void handleDataRead() = 0;

  /*
   * Derived class calls this method when there is data to be sent over the connection.
   * Base class will registerForWrite on the connection fd to be notified when it is writable.
   */
  int registerForWrite();

  // Endpoint read registration
  int unregisterEndpointForRead();
  int registerEndpointForRead();

  // Connection otions.
  ConnectionOptions* mOptions;

private:
  // Internal callback that is invoked when a read event happens on a
  // connected sate.
  void handleRead(EventLoop::Status);

  // Internal callback that is invoked when a write event happens on a
  // connected sate. In this routine we actually send the packets out.
  void handleWrite(EventLoop::Status);

  // A Connection can get closed by the connection class itself(because
  // of an io error). This is the method used to do that.
  void internalClose();

  // Connect status of this connection
  State mState;

  // Whether we are ready to read.
  ReadWriteState mReadState;

  // Whether we are ready to write.
  ReadWriteState mWriteState;

  // The user registered callbacks
  VCallback<NetworkErrorCode> mOnClose;

  // Our own callbacks that we register for internal reads/write events.
  VCallback<EventLoop::Status> mOnRead;
  VCallback<EventLoop::Status> mOnWrite;

  // Connection Endpoint
  ConnectionEndPoint* mEndpoint;

  // The underlying event loop
  EventLoop* mEventLoop;
  bool mCanCloseConnection;
};

#endif  // HERON_COMMON_SRC_CPP_NETWORK_BASECONNECTION_H_