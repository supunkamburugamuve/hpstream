#include "connection.h"

int32_t Connection::sendPacket(OutgoingPacket* packet) { return sendPacket(packet, NULL); }

int32_t Connection::sendPacket(OutgoingPacket* packet, VCallback<NetworkErrorCode> cb) {
  packet->PrepareForWriting();
  if (registerForWrite() != 0) return -1;
  mOutstandingPackets.push_back(std::make_pair(packet, std::move(cb)));
  mNumOutstandingPackets++;
  mNumOutstandingBytes += packet->GetTotalPacketSize();

  if (!hasCausedBackPressure()) {
    // Are we above the threshold?
    if (mNumOutstandingBytes >= systemHWMOutstandingBytes) {
      // Have we been above the threshold enough number of times?
      if (++mNumEnqueuesWithBufferFull > __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__) {
        mNumEnqueuesWithBufferFull = 0;
        if (mOnConnectionBufferFull) {
          mOnConnectionBufferFull(this);
        }
      }
    } else {
      mNumEnqueuesWithBufferFull = 0;
    }
  }
  return 0;
}

void Connection::registerForNewPacket(VCallback<IncomingPacket*> cb) {
  mOnNewPacket = std::move(cb);
}

int32_t Connection::registerForBackPressure(VCallback<Connection*> cbStarter,
                                             VCallback<Connection*> cbReliever) {
  mOnConnectionBufferFull = std::move(cbStarter);
  mOnConnectionBufferEmpty = std::move(cbReliever);
  return 0;
}

int32_t Connection::writeIntoIOVector(int32_t maxWrite, int32_t* toWrite) {
  int32_t bytesLeft = maxWrite;
  int32_t simulWrites =
      mIOVectorSize > mNumOutstandingPackets ? mNumOutstandingPackets : mIOVectorSize;
  *toWrite = 0;
  auto iter = mOutstandingPackets.begin();
  for (int32_t i = 0; i < simulWrites; ++i) {
    mIOVector[i].iov_base = iter->first->get_header() + iter->first->position_;
    mIOVector[i].iov_len = PacketHeader::get_packet_size(iter->first->get_header()) +
                           PacketHeader::header_size() - iter->first->position_;
    if (mIOVector[i].iov_len >= bytesLeft) {
      mIOVector[i].iov_len = bytesLeft;
    }
    bytesLeft -= mIOVector[i].iov_len;
    *toWrite = *toWrite + mIOVector[i].iov_len;
    if (bytesLeft <= 0) {
      return i + 1;
    }
    iter++;
  }
  return simulWrites;
}

void Connection::afterWriteIntoIOVector(int32_t simulWrites, ssize_t numWritten) {
  mNumOutstandingBytes -= numWritten;
  for (int32_t i = 0; i < simulWrites; ++i) {
    auto pr = mOutstandingPackets.front();
    if (numWritten >= (ssize_t)mIOVector[i].iov_len) {
      // This iov structure was completely written as instructed
      int32_t bytesLeftForThisPacket = PacketHeader::get_packet_size(pr.first->get_header()) +
                                         PacketHeader::header_size() - pr.first->position_;
      bytesLeftForThisPacket -= mIOVector[i].iov_len;
      if (bytesLeftForThisPacket == 0) {
        // This whole packet has been consumed
        mSentPackets.push_back(pr);
        mOutstandingPackets.pop_front();
        mNumOutstandingPackets--;
      } else {
        pr.first->position_ += mIOVector[i].iov_len;
      }
      numWritten -= mIOVector[i].iov_len;
    } else {
      // This iov structure has been partially sent out
      pr.first->position_ += numWritten;
      numWritten = 0;
    }
    if (numWritten <= 0) break;
  }

  // Check if we reduced the write buffer to something below the back
  // pressure threshold
  if (hasCausedBackPressure()) {
    // Signal pipe free
    if (mNumOutstandingBytes <= systemLWMOutstandingBytes) {
      mOnConnectionBufferEmpty(this);
    }
  }
}

bool Connection::stillHaveDataToWrite() {
  return !mOutstandingPackets.empty();
}

int32_t Connection::writeIntoEndPoint(int32_t fd) {
  int32_t bytesWritten = 0;
  while (1) {
    int32_t stillToWrite = mWriteBatchsize - bytesWritten;
    int32_t toWrite = 0;
    int32_t simulWrites = writeIntoIOVector(stillToWrite, &toWrite);

    ssize_t numWritten = ::writev(fd, mIOVector, simulWrites);
    if (numWritten >= 0) {
      afterWriteIntoIOVector(simulWrites, numWritten);
      bytesWritten += numWritten;
      if (bytesWritten >= mWriteBatchsize) {
        // We only write a at max this bytes at a time.
        // This is so that others can get a chance
        return 0;
      }
      if (numWritten < toWrite) {
        // writev would block otherwise
        return 0;
      }
      if (!stillHaveDataToWrite()) {
        // No more packets to write
        return 0;
      }
    } else {
      // some error happened in writev
      if (errno == EAGAIN || errno == EINTR) {
        // we need to retry the write again
        HPS_INFO("writev said to try again");
      } else {
        HPS_INFO("error happened in writev");
        return -1;
      }
    }
  }
}
