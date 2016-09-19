#include <glog/logging.h>
#include "connection.h"

#define __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__ 1048576

const sp_int32 __SYSTEM_NETWORK_READ_BATCH_SIZE__ = 1048576;           // 1M
const sp_int32 __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__ = 1048576;  // 1M

// This is the high water mark on the num of bytes that can be left outstanding on a connection
sp_int64 Connection::systemHWMOutstandingBytes = 1024 * 1024 * 100;  // 100M
// This is the low water mark on the num of bytes that can be left outstanding on a connection
sp_int64 Connection::systemLWMOutstandingBytes = 1024 * 1024 * 50;  // 50M

Connection::Connection(RDMAOptions *options, RDMAConnection *con, RDMAEventLoopNoneFD *loop)
    : BaseConnection(options, con, loop),
      mPendingWritePackets(0),
      mNumOutstandingBytes(0),
      mNumOutstandingPackets(0) {
  this->mRdmaConnection->setOnWriteComplete([this](uint32_t complets) {
    return this->writeComplete(complets); });
  this->mWriteBatchsize = __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__;
}

Connection::~Connection() { }

int32_t Connection::sendPacket(OutgoingPacket* packet) { return sendPacket(packet, NULL); }

int32_t Connection::sendPacket(OutgoingPacket* packet, VCallback<NetworkErrorCode> cb) {
  packet->PrepareForWriting();
  //if (registerForWrite() != 0) return -1;
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

int Connection::writeComplete(ssize_t numWritten) {
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
      mPendingWritePackets--;
      numWritten -= bytesLeftForThisPacket;
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
  return 0;
}

bool Connection::stillHaveDataToWrite() {
  return !mOutstandingPackets.empty();
}

int32_t Connection::writeIntoEndPoint(int fd) {
  uint32_t size_to_write = 0;
  char *buf = NULL;
  uint32_t current_write = 0, total_write = 0;
  int write_status;
  LOG(INFO) << "Write to endpoint";
  int current_packet = 0;
  for (auto iter = mOutstandingPackets.begin(); iter != mOutstandingPackets.end(); ++iter) {
    if (current_packet++ < mPendingWritePackets) {
      // we have written this packet already and waiting for write completion
      continue;
    }

    buf = iter->first->get_header() + iter->first->position_;
    size_to_write = PacketHeader::get_packet_size(iter->first->get_header()) +
                    PacketHeader::header_size() - iter->first->position_;

    // try to write the data
    write_status = writeData((uint8_t *) buf, size_to_write, &current_write);
    if (write_status) {
      LOG(ERROR) << "Failed to write the data";
      return write_status;
    }

    // we have written this fully
    if (current_write == size_to_write) {
      mPendingWritePackets++;
    } else {
      // partial write
      iter->first->position_ += current_write;
    }

    iter++;
    total_write += current_write;
    // we loop until we write everything we want to write is successful
    // and total written data is less than batch size
    if (!(current_write == size_to_write && total_write < mWriteBatchsize)) {
      break;
    }
  }
  return 0;
}

int32_t Connection::readFromEndPoint(int fd) {
  int32_t bytesRead = 0;
  LOG(INFO) << "Read from endpoint";
  while (1) {
    int32_t read_status = ReadPacket();
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

int32_t Connection::ReadPacket() {
  if (mIncomingPacket->data_ == NULL) {
    // We are still reading the header
    int32_t read_status =
        InternalPacketRead(mIncomingPacket->header_ + mIncomingPacket->position_,
                           PacketHeader::header_size() - mIncomingPacket->position_, &mIncomingPacket->position_);

    if (read_status != 0) {
      // Header read is either partial or had an error
      return read_status;

    } else {
      // Header just completed - some sanity checking of the header
      if (mIncomingPacket->max_packet_size_ != 0 &&
          PacketHeader::get_packet_size(mIncomingPacket->header_) > mIncomingPacket->max_packet_size_) {
        // Too large packet
        LOG(ERROR) << "Too large packet size " << PacketHeader::get_packet_size(mIncomingPacket->header_)
                   << ". We only accept packet sizes <= " << mIncomingPacket->max_packet_size_ << "\n";

        return -1;

      } else {
        // Create the data
        mIncomingPacket->data_ = new char[PacketHeader::get_packet_size(mIncomingPacket->header_)];

        // bzero(data_, PacketHeader::get_packet_size(header_));
        // reset the position to refer to the data_

        mIncomingPacket->position_ = 0;
      }
    }
  }

  // The header has been completely read. Read the data
  int32_t retval =
      InternalPacketRead(mIncomingPacket->data_ + mIncomingPacket->position_,
                         PacketHeader::get_packet_size(mIncomingPacket->header_) - mIncomingPacket->position_,
                         &mIncomingPacket->position_);
  if (retval == 0) {
    // Successfuly read the packet.
    mIncomingPacket->position_ = 0;
  }

  return retval;
}

int32_t Connection::InternalPacketRead(char* _buffer, uint32_t _size, uint32_t *position_) {
  char* current = _buffer;
  uint32_t to_read = _size;
  while (to_read > 0) {
    ssize_t num_read;
    num_read = readData((uint8_t *) current, to_read, (uint32_t *) &num_read);
    if (num_read > 0) {
      current = current + num_read;
      to_read = to_read - num_read;
      *position_ = *position_ + num_read;
    } else if (num_read == 0) {
      // remote end has done a shutdown.
      LOG(ERROR) << "Remote end has done a shutdown";
      return -1;
    } else {
      // read returned negative value.
      if (errno == EAGAIN) {
        // The read would block.
        // cout << "read syscall returned the EAGAIN errno " << errno << "\n";
        return 1;
      } else if (errno == EINTR) {
        // cout << "read syscall returned the EINTR errno " << errno << "\n";
        // the syscall encountered a signal before reading anything.
        // try again
        continue;
      } else {
        // something really bad happened. Bail out
        // try again
        LOG(ERROR) << "Something really bad happened while reading %d", errno;
        return -1;
      }
    }
  }
  return 0;
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
