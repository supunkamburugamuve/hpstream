#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cinttypes>
#include <cstring>

#include "hps.h"
#include "buffer.h"

Buffer::Buffer(uint8_t *buf, uint32_t buf_size, uint32_t no_bufs) {
  this->buf = buf;
  this->buf_size = buf_size / no_bufs;
  this->no_bufs = no_bufs;
  this->head = 0;
  this->tail = 0;
  this->data_head = 0;
  this->content_sizes = NULL;
  this->current_read_index = 0;
  this->buffers = NULL; // do error handling

  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&cond_empty, NULL);
  pthread_cond_init(&cond_full, NULL);
  Init();
}

int Buffer::acquireLock() {
  return pthread_mutex_lock(&lock);
}

int Buffer::releaseLock() {
  return pthread_mutex_unlock(&lock);
}

uint8_t * Buffer::GetBuffer(int i) {
  return buffers[i];
}

uint32_t Buffer::BufferSize() {
  return buf_size;
};

uint32_t Buffer::NoOfBuffers() {
  return no_bufs;
}

uint32_t Buffer::Head() {
  return head;
}

uint32_t Buffer::Tail() {
  return tail;
}

uint32_t Buffer::DataHead() {
  return data_head;
}

uint32_t Buffer::ContentSize(int i) {
  return content_sizes[i];
}

void Buffer::SetDataHead(uint32_t head) {
  this->data_head = head;
}

void Buffer::SetHead(uint32_t head) {
  this->head = head;
}

void Buffer::SetTail(uint32_t tail) {
  this->tail = tail;
}

uint32_t Buffer::CurrentReadIndex() {
  return this->current_read_index;
}

void Buffer::SetCurrentReadIndex(uint32_t indx) {
  this->current_read_index = indx;
}

int Buffer::Init() {
  uint32_t i = 0;
  this->buffers = (uint8_t **)malloc(sizeof(uint8_t *) * no_bufs);
  this->content_sizes = (uint32_t *)malloc(sizeof(uint32_t) * no_bufs);
  for (i = 0; i < no_bufs; i++) {
    this->buffers[i] = this->buf + buf_size * i;
  }
  this->head = 0;
  this->tail = 0;
  return 0;
}

int increment(int size, int current) {
  return size - 1 == current ? 0 : current + 1;
}

int Buffer::IncrementHead(uint32_t count) {
  this->head = (this->head + count) % this->no_bufs;
  return 0;
}

int Buffer::IncrementTail(uint32_t count) {
  this->tail = (this->tail + count) % this->no_bufs;
  // signal that we have an empty buffer
  pthread_cond_signal(&cond_empty);
  return 0;
}

int Buffer::IncrementDataHead(uint32_t count) {
  this->data_head = (this->data_head + count) % this->no_bufs;
  return 0;
}

void Buffer::Free() {
  if (this->buffers) {
    free(this->buffers);
  }
  if (this->content_sizes) {
    free(this->content_sizes);
  }
}

uint64_t Buffer::GetFreeSpace() {
  // get the total free space available
  int free_slots = this->no_bufs - abs(this->head - this->tail);
  return free_slots * this->buf_size;
}

// get the space ready to be received by user
uint64_t Buffer::GetReceiveReadySpace() {
  int ready_slots = this->no_bufs - abs(this->data_head - this->tail);
  return ready_slots * this->buf_size;
}

// get space ready to be posted to Hardware
uint64_t Buffer::GetSendReadySpace() {
  int ready_slots = this->no_bufs - abs(this->data_head - this->head);
  return ready_slots * this->buf_size;
}

int Buffer::waitFree() {
  return pthread_cond_wait(&cond_empty, &lock);
}

int Buffer::ReadData(uint8_t *buf, uint32_t size, uint32_t *read) {
  ssize_t ret = 0;
  // nothing to read
  if (tail == data_head) {
    *read = 0;
    return 0;
  }
  uint32_t tail = this->tail;
  uint32_t dataHead = this->data_head;
  uint32_t current_read_indx = this->current_read_index;
  // need to copy
  uint32_t need_copy = 0;
  // number of bytes copied
  uint32_t read_size = 0;
  HPS_INFO("Reading, tail= %d, dataHead= %d", tail, dataHead);
  while (read_size < size &&  tail != dataHead) {
    uint8_t *b = buffers[tail];
    uint32_t *r;
    // first read the amount of data in the buffer
    r = (uint32_t *) b;
    // now lets see how much data we need to copy from this buffer
    need_copy = (*r) - current_read_indx;
    // now lets see how much we can copy
    uint32_t can_copy = 0;
    uint32_t tmp_index = current_read_indx;
    HPS_INFO("Copy size=%" PRIu32 " read_size=%" PRIu32 " need_copy=%" PRIu32 " r=%" PRIu32 " read_idx=%" PRIu32, size, read_size, need_copy, r, current_read_indx);
    // we can copy everything from this buffer
    if (size - read_size >= need_copy) {
      HPS_INFO("Moving tail");
      can_copy = need_copy;
      current_read_indx = 0;
      // advance the tail pointer
      IncrementTail(1);
      tail = this->tail;
    } else {
      HPS_INFO("Not Moving tail");
      // we cannot copy everything from this buffer
      can_copy = size - read_size;
      current_read_indx += can_copy;
    }
    // next copy the buffer
    HPS_INFO("Memcopy %d %d", sizeof(uint32_t) + tmp_index, can_copy);
    memcpy(buf, b + sizeof(uint32_t) + tmp_index, can_copy);
    // now update
    HPS_INFO("Reading, tail= %d, dataHead= %d", tail, dataHead);
    read_size += can_copy;
  }

  *read = read_size;
  return 0;
}


