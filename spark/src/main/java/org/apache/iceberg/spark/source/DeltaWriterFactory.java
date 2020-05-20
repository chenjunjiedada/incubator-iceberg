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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;

public class DeltaWriterFactory {

  private final PartitionSpec spec;
  private final FileFormat format;
  private final LocationProvider locations;
  private final Map<String, String> properties;
  private final Broadcast<FileIO> io;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final long targetFileSize;

  DeltaWriterFactory(PartitionSpec spec, FileFormat format, LocationProvider locations,
                     Map<String, String> properties, Broadcast<FileIO> io,
                     Broadcast<EncryptionManager> encryptionManager, long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.locations = locations;
    this.properties = properties;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.targetFileSize = targetFileSize;
  }

  public DataWriter<InternalRow> createDeltaWriter(int partitionId, long taskId) {
    OutputFileFactory fileFactory = new OutputFileFactory(partitionId, taskId);
    AppenderFactory<InternalRow> appenderFactory = new SparkAppenderFactory(PositionDeleteFileReader.deleteSchema);
    return new PositionDeleteFileWriter(spec, format, appenderFactory, fileFactory, io.value(), targetFileSize);
  }

  private class SparkAppenderFactory implements AppenderFactory<InternalRow> {
    private final Schema schema;

    SparkAppenderFactory(Schema schema) {
      this.schema = schema;
    }

    @Override
    public FileAppender<InternalRow> newAppender(OutputFile file, FileFormat fileFormat) {
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(properties);
      try {
        switch (fileFormat) {
          case PARQUET:
            return Parquet.write(file)
                .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(schema, msgType))
                .setAll(properties)
                .metricsConfig(metricsConfig)
                .schema(schema)
                .overwrite()
                .build();

          case AVRO:
            return Avro.write(file)
                .createWriterFunc(ignored -> new SparkAvroWriter(schema))
                .setAll(properties)
                .schema(schema)
                .overwrite()
                .build();

          case ORC:
            return ORC.write(file)
                .createWriterFunc(SparkOrcWriter::new)
                .setAll(properties)
                .schema(schema)
                .overwrite()
                .build();

          default:
            throw new UnsupportedOperationException("Cannot write unknown format: " + fileFormat);
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }

  interface AppenderFactory<T> {
    FileAppender<T> newAppender(OutputFile file, FileFormat format);
  }

  class OutputFileFactory {
    private final int partitionId;
    private final long taskId;
    private final String uuid = UUID.randomUUID().toString();
    private int fileCount;

    OutputFileFactory(int partitionId, long taskId) {
      this.partitionId = partitionId;
      this.taskId = taskId;
      this.fileCount = 0;
    }

    private String generateFilename() {
      return format.addExtension(String.format("%05d-%d-%s-delta-%05d", partitionId, taskId, uuid, fileCount++));
    }

    /**
     * Generates EncryptedOutputFile for UnpartitionedWriter.
     */
    public EncryptedOutputFile newOutputFile() {
      OutputFile file = io.value().newOutputFile(locations.newDataLocation(generateFilename()));
      return encryptionManager.value().encrypt(file);
    }

    /**
     * Generates EncryptedOutputFile for PartitionedWriter.
     */
    public EncryptedOutputFile newOutputFile(PartitionKey key) {
      String newDataLocation = locations.newDataLocation(spec, key, generateFilename());
      OutputFile rawOutputFile = io.value().newOutputFile(newDataLocation);
      return encryptionManager.value().encrypt(rawOutputFile);
    }
  }
}
