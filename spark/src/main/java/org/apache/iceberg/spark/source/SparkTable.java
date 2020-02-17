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

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.util.SerializableConfiguration;

public class SparkTable {
  private Table icebergTable;
  private FileIO io;
  private Reader reader;

  SparkTable(JavaSparkContext sparkContext, Table table, DataSourceOptions options) {
    this.icebergTable = table;
    String caseSensitive = SparkSession.active().conf().get("spark.sql.caseSensitive");
    if (table.io() instanceof HadoopFileIO) {
      // we need to use Spark's SerializableConfiguration to avoid issues with Kryo serialization
      SerializableConfiguration conf = new SerializableConfiguration(((HadoopFileIO) table.io()).conf());
      this.io = new HadoopFileIO(conf::value);
    } else {
      this.io = table.io();
    }

    this.reader = new Reader(table, sparkContext.broadcast(io), sparkContext.broadcast(table.encryption()),
        Boolean.parseBoolean(caseSensitive), options);
  }


  public List<InputPartition<InternalRow>> planInputPartitions() {
    return reader.planInputPartitions();
  }

  public String name() {
    return this.icebergTable.toString();
  }

  public String normalizedName() {
    String[] nameParts = name().split("\\.");
    String tableName;

    if (nameParts.length > 2) {
      tableName = nameParts[nameParts.length - 2] + "." + nameParts[nameParts.length - 1];
    } else {
      tableName = name();
    }

    return tableName;
  }

  public Schema schema() {
    return icebergTable.schema();
  }

  public Schema metaSchema() {
    return new Schema(Schema.metaColumns);
  }

  public Table getIcebergTable() {
    return this.icebergTable;
  }

  public void pushFilters(Filter[] filters) {
    this.reader.pushFilters(filters);
  }
}
