#ifndef BUFFER_H_
#define BUFFER_H_

#include <unistd.h>

class Buffer {
public:
  Buffer(void *buf, uint64_t buf_size, uint32_t no_bufs);

  virtual ~Buffer();

  // increment the head
  bool IncrementHead();

  // increment the tail
  bool IncrementTail();

  // increment the data head
  bool IncrementDataHead();

  // get the free space available in the buffers
  uint64_t GetFreeSpace();

  // get the space ready to be received by user
  uint64_t GetReceiveReadySpace();

  // get space ready to be posted to Hardware
  uint64_t GetSendReadySpace();

  /** Getters and setters */
  uint8_t *GetBuffer(int i);
  int64_t BufferSize();
  uint32_t NoOfBuffers();
  uint32_t Head();
  uint32_t Tail();
  uint32_t DataHead();
  uint32_t ContentSize(int i);
  void SetDataHead(uint32_t head);
  void SetHead(uint32_t head);
  void SetTail(uint32_t tail);

  /** Free the buffer */
  void Free();

private:
  // part of the buffer allocated to this buffer
  void *buf;
  // the list of buffer pointers, these are pointers to
  // part of a large buffer allocated
  void **buffers;
  // list of buffer sizes
  uint32_t buf_size;
  // array of actual data sizes
  uint32_t *content_sizes;
  // buffers between tail and head are posted to RDMA operations
  // tail of the buffers that are being used
  // the buffers can be in a posted state, or received messages
  uint32_t tail;
  // head of the buffer
  uint32_t head;
  // buffers between head and data_head are with data from users
  // and these are ready to be posted
  // in case of receive, the data between tail and data_head are
  // received data, that needs to be consumed by the user
  uint32_t data_head;
  // no of buffers
  uint32_t no_bufs;
  // private methods
  int Init();
};

#endif /* BUFFER_H_ */
