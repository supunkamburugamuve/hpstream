#include "connection.h"

#define __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__ 64
#define __SYSTEM_NETWORK_READ_BATCH_SIZE__ 1024

Connection::Connection() {
  this->mRdmaConnection->setOnWriteComplete([this](uint32_t complets) {
    return this->afterWriteIntoIOVector(complets); });
}

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

void Connection::afterWriteIntoIOVector(ssize_t numWritten) {
  mNumOutstandingBytes -= numWritten;
  while (numWritten > 0) {
    auto pr = mOutstandingPackets.front();
    // This iov structure was completely written as instructed
    int32_t bytesLeftForThisPacket = PacketHeader::get_packet_size(pr.first->get_header()) +
                                     PacketHeader::header_size() - pr.first->position_;
    if (numWritten >= bytesLeftForThisPacket) {
        // This whole packet has been consumed
      mSentPackets.push_back(pr);
      mOutstandingPackets.pop_front();
      mNumOutstandingPackets--;
      numWritten -= bytesLeftForThisPacket;
    } else {
      // This iov structure has been partially sent out
      pr.first->position_ += numWritten;
      numWritten = 0;
    }
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

int32_t Connection::writeIntoEndPoint() {
  auto iter = mOutstandingPackets.begin();
  uint32_t size_to_write = 0;
  char *buf = NULL;
  uint32_t current_write = 0, total_write = 0;
  int write_status;
  do {
    buf = iter->first->get_header() + iter->first->position_;
    size_to_write = PacketHeader::get_packet_size(iter->first->get_header()) +
                    PacketHeader::header_size() - iter->first->position_;

    // try to write the data
    write_status = writeData((uint8_t *) buf, size_to_write, &current_write);
    if (write_status) {
      HPS_ERR("Failed to write the data");
      return write_status;
    }
    iter++;
    total_write += current_write;
    // we loop until we write everything we want to write is successful
    // and total written data is less than batch size
  } while (current_write == size_to_write && total_write < mWriteBatchsize);
  return 0;
}

int32_t Connection::readFromEndPoint() {
  int32_t bytesRead = 0;
  while (1) {
    int32_t read_status = mIncomingPacket->Read(this);
    if (read_status == 0) {
      // Packet was succcessfully read.
      IncomingPacket* packet = mIncomingPacket;
      mIncomingPacket = new IncomingPacket(mRdmaOptions->max_packet_size_);
      mReceivedPackets.push_back(packet);
      bytesRead += packet->GetTotalPacketSize();
      if (bytesRead >= __SYSTEM_NETWORK_READ_BATCH_SIZE__) {
        return 0;
      }
    } else if (read_status > 0) {
      // packet was read partially
      return 0;
    } else {
      return -1;
    }
  }
}

void Connection::handleDataRead() {
  while (!mReceivedPackets.empty()) {
    IncomingPacket* packet = mReceivedPackets.front();
    if (mOnNewPacket) {
      mOnNewPacket(packet);
    } else {
      delete packet;
    }
    mReceivedPackets.pop_front();
  }
}
