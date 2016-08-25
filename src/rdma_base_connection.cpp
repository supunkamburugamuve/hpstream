#include "rdma_base_connecion.h"

using namespace std;

int32_t BaseConnection::start() {
  if (mState != INIT) {
    HPS_ERR("Connection not in INIT State, hence cannot start");
    return -1;
  }
  if (mRdmaConnection->start()) {
    HPS_ERR("Could not start the rdma connection");
    return -1;
  }
  mState = CONNECTED;
  return 0;
}

void BaseConnection::closeConnection() {
  if (mState != CONNECTED) {
    // Nothing to do here
    HPS_ERR("Connection already closed, hence doing nothing");
    return;
  }
  mState = TO_BE_DISCONNECTED;
  internalClose();
}

void BaseConnection::internalClose() {
  if (mState != TO_BE_DISCONNECTED) return;
  if (!mCanCloseConnection) return;
  mState = DISCONNECTED;
}

string BaseConnection::getIPAddress() {
  return NULL;
}

