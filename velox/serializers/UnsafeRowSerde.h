/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <boost/crc.hpp>
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::batch {

class UnsafeRowVectorSerde : public BatchVectorSerde {
 public:
  UnsafeRowVectorSerde(
      velox::memory::MemoryPool* pool,
      const std::optional<std::vector<int>>& keys = std::nullopt)
      : BatchVectorSerde(),
        pool_(pool),
        rowBufferPtr_(nullptr),
        rowBuffer_(nullptr, 0),
        keys_{std::move(keys)} {}

  virtual ~UnsafeRowVectorSerde() = default;

  velox::BatchSerdeStatus serializeRow(
      const std::shared_ptr<RowVector>& vector,
      const vector_size_t row,
      std::string_view& resultBuffer) override;

  velox::BatchSerdeStatus deserializeBlock(
      const std::string_view& block,
      const std::shared_ptr<const RowType> type,
      bool includeKeys,
      std::shared_ptr<RowVector>* result) override;

 private:
  /// Used to serialized a group of keys (defined by keys_) into the current
  /// position of row serialization buffer
  /// \param vector the input vector
  /// \param row the row index
  /// \return
  std::optional<size_t> serializeKeys(
      const std::shared_ptr<RowVector>& vector,
      vector_size_t row);

  // Pool used for memory allocations
  velox::memory::MemoryPool* pool_;
  // Internally allocated buffer to be used by single row serialize
  BufferPtr rowBufferPtr_;
  std::string_view rowBuffer_;
  // List of keys that may need to be serialized along with the row
  const std::optional<std::vector<int>> keys_;
};

} // namespace facebook::velox::serializer::batch
