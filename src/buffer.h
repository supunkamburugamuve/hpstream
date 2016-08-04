#ifndef BUFFER_H_
#define BUFFER_H_

#include <cstdint>
#include <pthread.h>

class Buffer {
public:
  Buffer(uint8_t *buf, uint32_t buf_size, uint32_t no_bufs);

  // increment the head
  int IncrementSubmitted(uint32_t count);

  // increment the base
  int IncrementTail(uint32_t count);

  int IncrementFilled(uint32_t count);

  uint32_t GetFilledBuffers() {
    return filled_buffs;
  }

  uint32_t GetSubmittedBuffers() {
    return submitted_buffs;
  }

  // get the free space available in the buffers
  uint64_t GetAvailableWriteSpace();

  /** Getters and setters */
  uint8_t *GetBuffer(int i);
  uint32_t BufferSize();
  uint32_t NoOfBuffers();
  uint32_t Base();
  void SetBase(uint32_t tail);
  uint32_t CurrentReadIndex();
  void SetCurrentReadIndex(uint32_t indx);

  int ReadData(uint8_t *buf, uint32_t size, uint32_t *read);

  // wait until a buffer becomes available for write
  int waitFree();
  int acquireLock();
  int releaseLock();
  /** Free the buffer */
  void Free();

  uint32_t NextWriteIndex();

private:
  // place in the current buffer we read up to, this is needed to get the data out
  // of the buffers
  uint32_t current_read_index;
  // part of the buffer allocated to this buffer
  uint8_t *buf;
  // the list of buffer pointers, these are pointers to
  // part of a large buffer allocated
  uint8_t **buffers;
  // list of buffer sizes
  uint32_t buf_size;
  // array of actual data sizes
  uint32_t *content_sizes;
  // buffers between base and head are posted to RDMA operations
  // base of the buffers that are being used
  // the buffers can be in a posted state, or received messages
  uint32_t base;
  // no of buffers
  uint32_t no_bufs;

  // the thread lock
  pthread_mutex_t lock;
  // condition full
  pthread_cond_t cond_full;
  // condition empty
  pthread_cond_t cond_empty;
  // number of buffers submitted to RDMA
  uint32_t submitted_buffs;
  // number of buffers filled by RDMA
  uint32_t filled_buffs;
  // private methods
  int Init();
};

#endif /* BUFFER_H_ */
