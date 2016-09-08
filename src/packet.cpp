#include <netinet/in.h>
#include <glog/logging.h>
#include "packet.h"
#include <google/protobuf/message.h>
#include <google/protobuf/message_lite.h>

// PacketHeader static methods
void PacketHeader::set_packet_size(char* header, uint32_t _size) {
  uint32_t network_order = htonl(_size);
  memcpy(header, &network_order, sizeof(uint32_t));
}

uint32_t PacketHeader::get_packet_size(const char* header) {
  uint32_t network_order = *(reinterpret_cast<const uint32_t*>(header));
  return ntohl(network_order);
}

uint32_t PacketHeader::header_size() { return kSPPacketSize; }

// Constructor of the IncomingPacket. We only create the header buffer.
IncomingPacket::IncomingPacket(uint32_t _max_packet_size) {
  max_packet_size_ = _max_packet_size;
  position_ = 0;
  // bzero(header_, PacketHeader::size());
  data_ = NULL;
}

// Construct an incoming from a raw data buffer - used for tests only
IncomingPacket::IncomingPacket(char* _data) {
  memcpy(header_, _data, PacketHeader::header_size());
  data_ = new char[PacketHeader::get_packet_size(header_)];
  memcpy(data_, _data + PacketHeader::header_size(), PacketHeader::get_packet_size(header_));
  position_ = 0;
}

IncomingPacket::~IncomingPacket() { delete[] data_; }

int32_t IncomingPacket::UnPackInt(int32_t* i) {
  if (data_ == NULL) return -1;
  if (position_ + sizeof(int32_t) > PacketHeader::get_packet_size(header_)) return -1;
  uint32_t network_order;
  memcpy(&network_order, data_ + position_, sizeof(int32_t));
  position_ += sizeof(int32_t);
  *i = ntohl(network_order);
  return 0;
}

int32_t IncomingPacket::UnPackString(std::string* i) {
  int32_t size = 0;
  if (UnPackInt(&size) != 0) return -1;
  if (position_ + size > PacketHeader::get_packet_size(header_)) return -1;
  *i = std::string(data_ + position_, size);
  position_ += size;
  return 0;
}

int32_t IncomingPacket::UnPackProtocolBuffer(google::protobuf::Message* _proto) {
  int32_t sz;
  if (UnPackInt(&sz) != 0) return -1;
  if (position_ + sz > PacketHeader::get_packet_size(header_)) return -1;
  if (!_proto->ParseFromArray(data_ + position_, sz)) return -1;
  position_ += sz;
  return 0;
}

int32_t IncomingPacket::UnPackREQID(REQID* _rid) {
  if (position_ + REQID_size > PacketHeader::get_packet_size(header_)) return -1;
  _rid->assign(std::string(data_ + position_, REQID_size));
  position_ += REQID_size;
  return 0;
}

uint32_t IncomingPacket::GetTotalPacketSize() const {
  return PacketHeader::get_packet_size(header_) + kSPPacketSize;
}

void IncomingPacket::Reset() { position_ = 0; }

int32_t IncomingPacket::Read(Connection *con) {
  if (data_ == NULL) {
    // We are still reading the header
    int32_t read_status =
        InternalRead(con, header_ + position_, PacketHeader::header_size() - position_);

    if (read_status != 0) {
      // Header read is either partial or had an error
      return read_status;

    } else {
      // Header just completed - some sanity checking of the header
      if (max_packet_size_ != 0 && PacketHeader::get_packet_size(header_) > max_packet_size_) {
        // Too large packet
        LOG(ERROR) << "Too large packet size " << PacketHeader::get_packet_size(header_)
                   << ". We only accept packet sizes <= " << max_packet_size_ << "\n";

        return -1;

      } else {
        // Create the data
        data_ = new char[PacketHeader::get_packet_size(header_)];

        // bzero(data_, PacketHeader::get_packet_size(header_));
        // reset the position to refer to the data_

        position_ = 0;
      }
    }
  }

  // The header has been completely read. Read the data
  int32_t retval =
      InternalRead(con, data_ + position_, PacketHeader::get_packet_size(header_) - position_);
  if (retval == 0) {
    // Successfuly read the packet.
    position_ = 0;
  }

  return retval;
}

int32_t IncomingPacket::InternalRead(Connection *con, char* _buffer, uint32_t _size) {
  char* current = _buffer;
  uint32_t to_read = _size;
  while (to_read > 0) {
    ssize_t num_read;
    num_read = con->readData((uint8_t *) current, to_read, (uint32_t *) &num_read);
    if (num_read > 0) {
      current = current + num_read;
      to_read = to_read - num_read;
      position_ = position_ + num_read;
    } else if (num_read == 0) {
      // remote end has done a shutdown.
      HPS_ERR("Remote end has done a shutdown");
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
        HPS_ERR("Something really bad happened while reading %d", errno);
        return -1;
      }
    }
  }
  return 0;
}

OutgoingPacket::OutgoingPacket(uint32_t _packet_size) {
  total_packet_size_ = _packet_size + PacketHeader::header_size();
  data_ = new char[total_packet_size_];
  PacketHeader::set_packet_size(data_, _packet_size);
  position_ = PacketHeader::header_size();
}

OutgoingPacket::~OutgoingPacket() { delete[] data_; }

uint32_t OutgoingPacket::GetTotalPacketSize() const { return total_packet_size_; }

uint32_t OutgoingPacket::GetBytesFilled() const { return position_; }

uint32_t OutgoingPacket::GetBytesLeft() const { return total_packet_size_ - position_; }

int32_t OutgoingPacket::PackInt(const int32_t& i) {
  if (sizeof(int32_t) + position_ > total_packet_size_) {
    return -1;
  }
  int32_t network_order = htonl(i);
  memcpy(data_ + position_, &network_order, sizeof(int32_t));
  position_ += sizeof(int32_t);
  return 0;
}

uint32_t OutgoingPacket::SizeRequiredToPackProtocolBuffer(int32_t _byte_size) {
  return sizeof(int32_t) + _byte_size;
}

int32_t OutgoingPacket::PackProtocolBuffer(const google::protobuf::Message& _proto,
                                            int32_t _byte_size) {
  if (PackInt(_byte_size) != 0) return -1;
  if (_byte_size + position_ > total_packet_size_) {
    return -1;
  }
  if (!_proto.SerializeToArray(data_ + position_, _byte_size)) return -1;
  position_ += _byte_size;
  return 0;
}

int32_t OutgoingPacket::PackREQID(const REQID& _rid) {
  if (REQID_size + position_ > total_packet_size_) {
    return -1;
  }
  memcpy(data_ + position_, _rid.c_str(), REQID_size);
  position_ += REQID_size;
  return 0;
}

uint32_t OutgoingPacket::SizeRequiredToPackString(const std::string& _input) {
  return sizeof(uint32_t) + _input.size();
}

int32_t OutgoingPacket::PackString(const string& i) {
  if (sizeof(uint32_t) + i.size() + position_ > total_packet_size_) {
    return -1;
  }
  PackInt(i.size());
  memcpy(data_ + position_, i.c_str(), i.size());
  position_ += i.size();
  return 0;
}

void OutgoingPacket::PrepareForWriting() {
  CHECK(position_ == total_packet_size_);
  position_ = 0;
}

uint32_t OutgoingPacket::Write(int32_t _fd) {
  while (position_ < total_packet_size_) {
    ssize_t num_written = write(_fd, data_ + position_, total_packet_size_ - position_);
    if (num_written > 0) {
      position_ = position_ + num_written;
    } else {
      if (errno == EAGAIN) {
        LOG(INFO) << "syscall write returned EAGAIN errno " << errno << "\n";
        return 1;
      } else if (errno == EINTR) {
        LOG(INFO) << "syscall write returned EINTR errno " << errno << "\n";
        continue;
      } else {
        LOG(ERROR) << "syscall write returned errno " << errno << "\n";
        return -1;
      }
    }
  }

  return 0;
}
