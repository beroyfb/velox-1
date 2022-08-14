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

#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/init/Init.h>

#include "velox/serializers/UnsafeRowSerde.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::row {
namespace {

using namespace facebook::velox::test;

/// This test evaluate UnsafeRowBatchSerde
class UnsafeRowBatchSerdeFuzzTests : public ::testing::Test {
 public:
  UnsafeRowBatchSerdeFuzzTests() {}

  void resetBuffer() {
    blockUsedSize_ = 0;
  }

  void addRowToBuffer(const std::string_view row) {
    if (blockUsedSize_ + row.size() > blockSize_) {
      // Reallocate block buffer if the rows are overflowing
      size_t newBlockSize = (blockSize_ + row.size()) * 2;
      auto newBufferPtr =
          AlignedBuffer::allocate<char>(newBlockSize, pool_.get(), true);
      char* newBuffer = newBufferPtr->asMutable<char>();
      std::memcpy(newBuffer, buffer_, blockUsedSize_);
      // Switch the old block with the new one
      bufferPtr_ = newBufferPtr;
      buffer_ = newBuffer;
      blockSize_ = newBlockSize;
    }
    // Add the row to the block
    std::memcpy(buffer_ + blockUsedSize_, row.data(), row.size());
    blockUsedSize_ += row.size();
  }

  void testSerde(
      const RowVectorPtr& inputVector,
      std::optional<std::vector<int>> keys,
      RowVectorPtr& outputVector) {
    velox::serializer::batch::UnsafeRowVectorSerde serde(pool_.get(), keys);
    std::string_view serializedRow;
    // Serialize one row at a time and add to block
    for (vector_size_t index = 0; index < inputVector->size(); index++) {
      auto err = serde.serializeRow(inputVector, index, serializedRow);
      ASSERT_EQ(err, velox::BatchSerdeStatus::Success);
      addRowToBuffer(serializedRow);
    }

    // Use block deserializer to deserialize all the rows
    auto err = serde.deserializeBlock(
        std::string_view(buffer_, blockUsedSize_),
        std::dynamic_pointer_cast<const RowType>(inputVector->type()),
        true,
        &outputVector);
    ASSERT_EQ(err, velox::BatchSerdeStatus::Success);
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
  BufferPtr bufferPtr_ =
      AlignedBuffer::allocate<char>(kInitialBlockSize, pool_.get(), true);
  char* buffer_ = bufferPtr_->asMutable<char>();
  size_t blockSize_ = kInitialBlockSize;
  size_t blockUsedSize_ = 0;
  static constexpr size_t kInitialBlockSize = 10 << 10; // 10k
};

TEST_F(UnsafeRowBatchSerdeFuzzTests, simpleTypeRoundTripTest) {
  auto rowType = ROW(
      {BOOLEAN(),
       TINYINT(),
       SMALLINT(),
       INTEGER(),
       BIGINT(),
       REAL(),
       DOUBLE(),
       VARCHAR(),
       TIMESTAMP(),
       ROW({VARCHAR(), INTEGER()}),
       ARRAY(INTEGER()),
       ARRAY(INTEGER()),
       MAP(VARCHAR(), ARRAY(INTEGER()))});

  VectorFuzzer::Options opts;
  opts.vectorSize = 5;
  opts.nullRatio = 0.1;
  opts.containerHasNulls = false;
  opts.dictionaryHasNulls = false;
  opts.stringVariableLength = true;
  opts.stringLength = 20;
  // Spark uses microseconds to store timestamp
  opts.useMicrosecondPrecisionTimestamp = true;
  opts.containerLength = 65;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  SCOPED_TRACE(fmt::format("seed: {}", seed));
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  const auto iterations = 5;
  for (size_t i = 0; i < iterations; ++i) {
    resetBuffer();
    const auto& inputVector = fuzzer.fuzzRow(rowType);
    RowVectorPtr outputVector;
    testSerde(
        inputVector,
        std::vector<int>{0, 1, 9, 10} /* 2 fix and 2 var-size keys */,
        outputVector);
    // Compare the input and output vectors
    assertEqualVectors(inputVector, outputVector);
  }
}

} // namespace
} // namespace facebook::velox::row
