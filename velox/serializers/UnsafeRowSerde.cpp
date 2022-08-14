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
#include "velox/serializers/UnsafeRowSerde.h"
#include "velox/row/UnsafeRowDeserializer.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"

namespace facebook::velox::serializer::batch {
namespace {
/// Finds row in a serializer vector by detecting the row (and key) sizes
/// \param block block of bytes
/// \param includesKeys true if the block has serialized keys as well
/// in which case it is structured as follow: keySize keyBytes rowSize rowBytes
/// \param rowPointers returned row pointers
/// \return
bool collectRowPointers(
    const std::string_view& block,
    bool includesKeys,
    std::vector<std::optional<std::string_view>>& rowPointers) {
  char* buffer = const_cast<char*>(block.data());
  char* originalBuffer = buffer;

  // Precess until the end of block
  ssize_t remainingSize = block.size();
  while (remainingSize > 0) {
    // Skip keys if any
    if (includesKeys) {
      size_t keysSize = *((size_t*)buffer);
      if (keysSize < 0 || keysSize > remainingSize) {
        return false;
      }
      buffer += sizeof(keysSize);
      buffer += keysSize;
    }
    // Read the row size
    size_t rowSize = *((size_t*)buffer);
    if (rowSize < 0 || rowSize > remainingSize) {
      return false;
    }
    buffer += sizeof(rowSize);
    rowPointers.emplace_back(std::string_view(buffer, rowSize));
    buffer += rowSize;
    remainingSize = block.size() - (buffer - originalBuffer);
    if (remainingSize < 0) {
      return false;
    }
  }
  return true;
}
} // namespace

std::optional<size_t> UnsafeRowVectorSerde::serializeKeys(
    const std::shared_ptr<RowVector>& vector,
    vector_size_t row) {
  char* buffer = const_cast<char*>(rowBuffer_.data());
  char* originalBuffer = buffer;
  size_t keySize = 0;
  buffer += sizeof(keySize);

  auto fieldTypes =
      std::dynamic_pointer_cast<const RowType>(vector->type())->children();
  // Traverse the keys and serialize them
  for (auto index : keys_.value()) {
    if (index < 0 || index > fieldTypes.size()) {
      return std::nullopt;
    }
    auto nextKeySize =
        velox::row::UnsafeRowDynamicSerializer::serialize(
            fieldTypes[index], vector->children()[index], buffer, row)
            .value_or(0);
    if (fieldTypes[index]->isFixedWidth()) {
      nextKeySize = velox::row::UnsafeRow::kFieldWidthBytes;
    }
    // Moving buffer pointer forward and updating the size
    keySize += nextKeySize;
    buffer += nextKeySize;
  }
  // Writing the composite key size
  *((size_t*)originalBuffer) = keySize;
  return buffer - originalBuffer;
}

velox::BatchSerdeStatus UnsafeRowVectorSerde::serializeRow(
    const std::shared_ptr<RowVector>& vector,
    const vector_size_t row,
    std::string_view& resultBuffer) {
  // Get the row size without writing to the buffer
  size_t rowSize = velox::row::UnsafeRowDynamicSerializer::getSizeRow(
      vector->type(), vector.get(), row);

  // Check if the current allocated buffer is large enough for this row
  if (rowBuffer_.data() == nullptr || rowSize > rowBuffer_.size()) {
    // Need more memory if we are serializing keys as well
    // We allocate at least twice the needed size so if the new rows are
    // bigger we do not have re-allocate
    // In case there are keys we allocate three times the size
    // The previously allocated buffer will be deallocated automatically
    auto allocationSize = keys_.has_value() ? rowSize * 3 : rowSize * 2;
    rowBufferPtr_ = AlignedBuffer::allocate<char>(allocationSize, pool_, true);

    rowBuffer_ =
        std::string_view(rowBufferPtr_->asMutable<char>(), allocationSize);
    std::memset(const_cast<char*>(rowBuffer_.data()), 0, allocationSize);
  }

  size_t rowOffset = 0;
  // first, writing the keys if any
  if (keys_.has_value()) {
    auto offset = serializeKeys(vector, row);
    if (!offset.has_value()) {
      return velox::BatchSerdeStatus::KeysSerializationFailed;
    }
    rowOffset += offset.value();
  }

  // Preparing the buffer location for the row itself and its size
  char* rowBuffer = const_cast<char*>(rowBuffer_.data()) + rowOffset;
  char* originalRowBuffer = rowBuffer;

  // Serialize the row and after make room for its size
  rowBuffer += sizeof(size_t);
  auto size = velox::row::UnsafeRowDynamicSerializer::serialize(
                  vector->type(), vector, rowBuffer, row)
                  .value_or(0);

  // Writing the size
  *((size_t*)originalRowBuffer) = size;

  // Return the result
  resultBuffer =
      std::string_view(rowBuffer_.data(), rowBuffer + size - rowBuffer_.data());
  return velox::BatchSerdeStatus::Success;
}

velox::BatchSerdeStatus UnsafeRowVectorSerde::deserializeBlock(
    const std::string_view& block,
    const std::shared_ptr<const RowType> type,
    bool includeKeys,
    std::shared_ptr<RowVector>* result) {
  // Collect row pointers
  std::vector<std::optional<std::string_view>> rowPointers;
  if (!collectRowPointers(block, includeKeys, rowPointers)) {
    return velox::BatchSerdeStatus::KeysSerializationFailed;
  }

  // Use row pointer to finish deserialization
  auto resultVector =
      velox::row::UnsafeRowDynamicVectorDeserializer::deserializeComplex(
          rowPointers, type, pool_);
  if (!resultVector) {
    return velox::BatchSerdeStatus::DeserializationFailed;
  }

  // Assign result
  *result = std::dynamic_pointer_cast<RowVector>(resultVector);

  return velox::BatchSerdeStatus::Success;
}
} // namespace facebook::velox::serializer::batch
