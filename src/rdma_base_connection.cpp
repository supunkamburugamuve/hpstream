#include <glog/logging.h>
#include "rdma_base_connecion.h"

BaseConnection::BaseConnection(RDMAOptions *options, RDMAConnection *con,
                               RDMAEventLoopNoneFD *loop)
    : mRdmaConnection(con), mRdmaOptions(options), mEventLoop(loop){
}

int32_t BaseConnection::start() {
  if (mState != INIT) {
    LOG(ERROR) << "Connection not in INIT State, hence cannot start";
    return -1;
  }

  mOnWrite = [this](void) { return this->handleWrite(); };
  mOnRead = [this](void) { return this->handleRead(); };

  mRdmaConnection->registerRead(mOnRead);
  mRdmaConnection->registerWrite(mOnWrite);

  if (mRdmaConnection->start()) {
    LOG(ERROR) << "Could not start the rdma connection";
    return -1;
  }
  mState = CONNECTED;
  return 0;
}

void BaseConnection::closeConnection() {
  if (mState != CONNECTED) {
    // Nothing to do here
    LOG(ERROR) << "Connection already closed, hence doing nothing";
    return;
  }
  mState = TO_BE_DISCONNECTED;
  internalClose();
}

void BaseConnection::internalClose() {
  if (mState != TO_BE_DISCONNECTED) return;
  if (!mCanCloseConnection) return;
  mState = DISCONNECTED;

  if (mState != TO_BE_DISCONNECTED) return;
  if (!mCanCloseConnection) return;
  mState = DISCONNECTED;
  // for now lets close
  // todo we need to have a good look
  mRdmaConnection->closeConnection();
}

std::string BaseConnection::getIPAddress() {
  std::string addr_result = "";
  if (mState != CONNECTED) {
    LOG(ERROR) << "Not in connected state, cannot get port";
    return addr_result;
  }
  char *address = mRdmaConnection->getIPAddress();
  addr_result = string(address);
  return addr_result;
}

int32_t BaseConnection::getPort() {
  if (mState != CONNECTED) {
    LOG(ERROR) << "Not in connected state, cannot get port";
    return -1;
  }
  return mRdmaConnection->getPort();
}

void BaseConnection::registerForClose(VCallback<NetworkErrorCode> cb) {
  mOnClose = std::move(cb);
}

int BaseConnection::readData(uint8_t *buf, uint32_t size, uint32_t *read) {
  return mRdmaConnection->ReadData(buf, size, read);
}

int BaseConnection::writeData(uint8_t *buf, uint32_t size, uint32_t *write) {
  return mRdmaConnection->WriteData(buf, size, write);
}

// Note that we hold the mutex when we come to this function
void BaseConnection::handleWrite() {
  mWriteState = NOTREGISTERED;

  if (mState != CONNECTED) return;

  int32_t writeStatus = writeIntoEndPoint();
  if (writeStatus < 0) {
    mWriteState = ERROR;
    mState = TO_BE_DISCONNECTED;
  }
  if (mState == CONNECTED && mWriteState == NOTREGISTERED && stillHaveDataToWrite()) {
    mWriteState = NOTREADY;
  }

  bool prevValue = mCanCloseConnection;
  mCanCloseConnection = false;
  handleDataWritten();
  mCanCloseConnection = prevValue;
  if (mState != CONNECTED) {
    internalClose();
  }
}

void BaseConnection::handleRead() {
  mReadState = READY;
  int32_t readStatus = readFromEndPoint();
  if (readStatus >= 0) {
    mReadState = NOTREADY;
  } else {
    mReadState = ERROR;
    mState = TO_BE_DISCONNECTED;
  }

  bool prevValue = mCanCloseConnection;
  mCanCloseConnection = false;
  handleDataRead();
  mCanCloseConnection = prevValue;
  if (mState != CONNECTED) {
    internalClose();
  }
}

