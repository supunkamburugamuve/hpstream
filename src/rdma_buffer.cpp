#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cinttypes>
#include <cstring>

#include "hps.h"
#include "rdma_buffer.h"

RDMABuffer::RDMABuffer(uint8_t *buf, uint32_t buf_size, uint32_t no_bufs) {
  this->buf = buf;
  this->buf_size = buf_size / no_bufs;
  this->no_bufs = no_bufs;
  this->base = 0;
  this->content_sizes = NULL;
  this->current_read_index = 0;
  this->buffers = NULL; // do error handling
  this->submitted_buffs = 0;
  this->filled_buffs = 0;

  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&cond_empty, NULL);
  pthread_cond_init(&cond_full, NULL);
  Init();
}

int RDMABuffer::acquireLock() {
  return pthread_mutex_lock(&lock);
}

int RDMABuffer::releaseLock() {
  return pthread_mutex_unlock(&lock);
}

uint8_t * RDMABuffer::GetBuffer(int i) {
  return buffers[i];
}

uint32_t RDMABuffer::BufferSize() {
  return buf_size;
};

uint32_t RDMABuffer::NoOfBuffers() {
  return no_bufs;
}

uint32_t RDMABuffer::Base() {
  return base;
}

void RDMABuffer::SetBase(uint32_t tail) {
  this->base = tail;
}

uint32_t RDMABuffer::CurrentReadIndex() {
  return this->current_read_index;
}

void RDMABuffer::SetCurrentReadIndex(uint32_t indx) {
  this->current_read_index = indx;
}

int RDMABuffer::Init() {
  uint32_t i = 0;
  this->buffers = (uint8_t **)malloc(sizeof(uint8_t *) * no_bufs);
  this->content_sizes = (uint32_t *)malloc(sizeof(uint32_t) * no_bufs);
  for (i = 0; i < no_bufs; i++) {
    this->buffers[i] = this->buf + buf_size * i;
  }
  this->base = 0;
  return 0;
}

int increment(int size, int current) {
  return size - 1 == current ? 0 : current + 1;
}

int RDMABuffer::IncrementFilled(uint32_t count) {
  uint32_t temp = this->filled_buffs + count;
  if (temp > this->no_bufs) {
    HPS_ERR("Failed to increment the submitted, inconsistant state temp=%" PRIu32 " submitted=%" PRId32 " filled=%" PRId32, temp, this->submitted_buffs, this->filled_buffs);
    return 1;
  }
  this->filled_buffs = temp;
  return 0;
}

int RDMABuffer::IncrementSubmitted(uint32_t count) {
  uint32_t temp = this->submitted_buffs + count;
  if (temp > this->no_bufs) {
    HPS_ERR("Failed to increment the submitted, inconsistant state temp=%" PRIu32 " submitted=%" PRId32 " filled=%" PRId32, temp, this->submitted_buffs, this->filled_buffs);
    return 1;
  }
  this->submitted_buffs = temp;
  return 0;
}

int RDMABuffer::IncrementTail(uint32_t count) {
  if (this->filled_buffs - count < 0 || this->submitted_buffs - count < 0) {
    HPS_ERR("Failed to decrement the buffer, inconsistent state");
    return 1;
  }
  this->base = (this->base + count) % this->no_bufs;
  // dec submitted and filled
  this->submitted_buffs -= count;
  this->filled_buffs -= count;
  // signal that we have an empty buffer
  pthread_cond_signal(&cond_empty);
  return 0;
}

void RDMABuffer::Free() {
  if (this->buffers) {
    free(this->buffers);
  }
  if (this->content_sizes) {
    free(this->content_sizes);
  }
}

uint64_t RDMABuffer::GetAvailableWriteSpace() {
  // get the total free space available
  int free_slots = this->no_bufs - this->filled_buffs;
  return free_slots * this->buf_size;
}

uint32_t RDMABuffer::NextWriteIndex() {
  return (base + this->filled_buffs - 1) % this->no_bufs;
}

int RDMABuffer::waitFree() {
  return pthread_cond_wait(&cond_empty, &lock);
}

int RDMABuffer::ReadData(uint8_t *buf, uint32_t size, uint32_t *read) {
  // nothing to read
  if (filled_buffs == 0) {
    *read = 0;
    return 0;
  }
  uint32_t tail = this->base;
  uint32_t buffers_filled = this->filled_buffs;
  uint32_t current_read_indx = this->current_read_index;
  // need to copy
  uint32_t need_copy = 0;
  // number of bytes copied
  uint32_t read_size = 0;
//  HPS_INFO("Reading, base= %d, dataHead= %d", tail, buffers_filled);
  while (read_size < size &&  buffers_filled > 0) {
    uint8_t *b = buffers[tail];
    uint32_t *r;
    // first read the amount of data in the buffer
    r = (uint32_t *) b;
    // now lets see how much data we need to copy from this buffer
    need_copy = (*r) - current_read_indx;
    // now lets see how much we can copy
    uint32_t can_copy = 0;
    uint32_t tmp_index = current_read_indx;
//    HPS_INFO("Copy size=%" PRIu32 " read_size=%" PRIu32 " need_copy=%" PRIu32 " r=%" PRIu32 " read_idx=%" PRIu32, size, read_size, need_copy, *r, current_read_indx);
    // we can copy everything from this buffer
    if (size - read_size >= need_copy) {
//      HPS_INFO("Moving base");
      can_copy = need_copy;
      current_read_indx = 0;
      // advance the base pointer
      IncrementTail(1);
      buffers_filled--;
      tail = this->base;
    } else {
//      HPS_INFO("Not Moving base");
      // we cannot copy everything from this buffer
      can_copy = size - read_size;
      current_read_indx += can_copy;
    }
    // next copy the buffer
//    HPS_INFO("Memcopy %d %d", sizeof(uint32_t) + tmp_index, can_copy);
//    uint32_t *buffer = (uint32_t *) (b + sizeof(uint32_t));
//    int i = 0;
//    for (i = 0; i < ((buf_size - 4) / sizeof(int)); i++) {
//      printf("%d ", buffer[i]);
//    }
//    printf("\nwritten=%d \n", i);

    memcpy(buf + read_size, b + sizeof(uint32_t) + tmp_index, can_copy);
    // now update
//    HPS_INFO("Reading, base= %d, dataHead= %d read_size=%" PRId32, tail, buffers_filled, read_size);
    read_size += can_copy;
  }

  *read = read_size;
  return 0;
}

