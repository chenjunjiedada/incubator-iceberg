/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.source;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;

public class InternalRowWithMeta extends GenericInternalRow {
  private InternalRow internalRow;
  private UTF8String filePath;
  private Long rowIndex;

  public InternalRowWithMeta(InternalRow internalRow, String filePath, Long rowIndex) {
    this.internalRow = internalRow;
    this.filePath = UTF8String.fromString(filePath);
    this.rowIndex = rowIndex;
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    int index = internalRow.numFields();

    if (ordinal == index) {
      return filePath;
    } else if (ordinal == index + 1) {
      return rowIndex;
    } else {
      return internalRow.get(ordinal, dataType);
    }
  }

  @Override
  public String toString() {
    return filePath.toString() + rowIndex.toString() + internalRow.toString();
  }
}