#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdint.h>

#include "buffer.h"

Buffer::Buffer(uint8_t *buf, uint64_t buf_size, uint32_t no_bufs) {
  this->buf = buf;6
  this->buf_size = buf_size;
  this->no_bufs = no_bufs;
  this->head = 0;
  this->tail = 0;
  this->data_head = 0;
  this->content_sizes = NULL;
  this->buffers = NULL; // do error handling
  Init();
}

uint8_t * Buffer::GetBuffer(int i) {
  return buffers[i];
}

int64_t Buffer::BufferSize() {
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

int Buffer::Init() {
  uint32_t i = 0;
  this->buffers = (uint8_t **)malloc(sizeof(uint8_t *) * no_bufs);
  this->content_sizes = (uint32_t *)malloc(sizeof(uint32_t) * no_bufs);
  this->buf_size = this->buf_size / this->no_bufs;
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

bool Buffer::IncrementHead() {
  uint32_t tail_previous = this->tail == 0 ? this->no_bufs - 1 : this->tail - 1;
  if (this->head != tail_previous) {
    this->head = this->head != this->no_bufs - 1 ? this->head + 1 : 0;
    return true;
  } else {
    return false;
  }
}

bool Buffer::IncrementTail() {
  if (this->head != this->tail) {
    this->tail = this->tail != 0 ? this->tail + 1 : this->no_bufs -1;
    return true;
  } else {
    return false;
  }
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


