#pragma once

#include <string>

#include "chunk/BitStream.hpp"
#include "chunk/ChunkAppenderInterface.hpp"
#include "chunk/ChunkInterface.hpp"
#include "chunk/ChunkIteratorInterface.hpp"
#include "chunk/XORIterator.hpp"

// XOR chunk data
// ┌──────────────────────┬───────────────┬───────────────┬──────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┬─────┬──────────────────────┬──────────────────────┬──────────────────┐
// │ num_samples <uint16> │ ts_0 <varint> │ v_0 <float64> │ ts_1_delta <uvarint> │ v_1_xor <varbit_xor> │ ts_2_dod <varbit_ts> │ v_2_xor <varbit_xor> │ ... │ ts_n_dod <varbit_ts> │ v_n_xor <varbit_xor> │ padding <x bits> │
// └──────────────────────┴───────────────┴───────────────┴──────────────────────┴──────────────────────┴──────────────────────┴──────────────────────┴─────┴──────────────────────┴──────────────────────┴──────────────────┘
// Notes:
// ts is the timestamp, v is the value.
//... means to repeat the previous two fields as needed, with n starting at 2 and going up to num_samples – 1. <uint16> has 2 bytes in big-endian order.
//<varint> and <uvarint> have 1 to 10 bytes each.
// ts_1_delta is ts_1 – ts_0.
// ts_n_dod is the “delta of deltas” of timestamps, i.e. (ts_n – ts_n-1) – (ts_n-1 – ts_n-2). v_n_xor is the result of v_n XOR v_n-1.
// <varbit_xor> is a specific variable bitwidth encoding of the result of XORing the current and the previous value.
// It has between 1 bit and 77 bits. See code for details.
//<varbit_ts> is a specific variable bitwidth encoding for the “delta of deltas” of timestamps (signed integers that are ideally small).
// It has between 1 and 68 bits. see code for details.
// padding of 0 to 7 bits so that the whole chunk data is byte-aligned.
// The chunk can have as few as one sample, i.e. ts_1, v_1, etc. are optional.

namespace tsdb {
namespace chunk {

class XORChunk : public ChunkInterface {
 public:
  BitStream bstream;
  bool read_mode;
  uint64_t size_;

  uint8_t *rbuf_;

 public:
  // The first two bytes store the num of samples using big endian
  XORChunk();

  // Reserve a header space.
  XORChunk(int header_size);

  XORChunk(const uint8_t *stream_ptr, uint64_t size, bool copy = false);

  ~XORChunk();

  const uint8_t *bytes();

  const uint8_t *all_bytes();

  uint8_t encoding();

  std::unique_ptr<ChunkAppenderInterface> appender();

  std::unique_ptr<ChunkIteratorInterface> iterator();

  std::unique_ptr<XORIterator> xor_iterator();

  int num_samples();

  uint64_t size();
  uint64_t all_size();
};

}  // namespace chunk
}  // namespace tsdb
