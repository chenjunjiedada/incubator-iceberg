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
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

class PositionDeleteFileWriter implements DataWriter<InternalRow> {
  private static final int ROWS_DIVISOR = 1000;

  private final List<DataFile> completedFiles = Lists.newArrayList();
  private final PartitionSpec spec;
  private final FileFormat format;
  private final DeltaWriterFactory.AppenderFactory<InternalRow> appenderFactory;
  private final DeltaWriterFactory.OutputFileFactory fileFactory;
  private final FileIO fileIo;
  private final long targetFileSize;
  private PartitionKey currentKey = null;
  private FileAppender<InternalRow> currentAppender = null;
  private EncryptedOutputFile currentFile = null;
  private long currentRows = 0;

  PositionDeleteFileWriter(PartitionSpec spec, FileFormat format,
                           DeltaWriterFactory.AppenderFactory<InternalRow> appenderFactory,
                           DeltaWriterFactory.OutputFileFactory fileFactory, FileIO fileIo, long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.fileIo = fileIo;
    this.targetFileSize = targetFileSize;
  }

  @Override
  public void write(InternalRow row) throws IOException {
    //TODO: ORC file now not support target file size before closed
    if  (!format.equals(FileFormat.ORC) &&
        currentRows % ROWS_DIVISOR == 0 && currentAppender.length() >= targetFileSize) {
      closeCurrent();
      openCurrent();
    }

    currentAppender.add(row);
    currentRows++;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    closeCurrent();

    return new TaskCommit(completedFiles);
  }

  @Override
  public void abort() throws IOException {
    closeCurrent();

    // clean up files created by this writer
    Tasks.foreach(completedFiles)
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> fileIo.deleteFile(file.path().toString()));
  }

  protected void openCurrent() {
    if (spec.fields().size() == 0) {
      // unpartitioned
      currentFile = fileFactory.newOutputFile();
    } else {
      // partitioned
      currentFile = fileFactory.newOutputFile(currentKey);
    }
    currentAppender = appenderFactory.newAppender(currentFile.encryptingOutputFile(), format);
    currentRows = 0;
  }

  protected void setCurrentKey(PartitionKey currentKey) {
    this.currentKey = currentKey;
  }

  protected void closeCurrent() throws IOException {
    if (currentAppender != null) {
      currentAppender.close();
      // metrics are only valid after the appender is closed
      Metrics metrics = currentAppender.metrics();
      long fileSizeInBytes = currentAppender.length();
      List<Long> splitOffsets = currentAppender.splitOffsets();
      this.currentAppender = null;

      if (metrics.recordCount() == 0L) {
        fileIo.deleteFile(currentFile.encryptingOutputFile());
      } else {
        DataFile dataFile = DataFiles.builder(spec)
            .withEncryptionKeyMetadata(currentFile.keyMetadata())
            .withPath(currentFile.encryptingOutputFile().location())
            .withFileSizeInBytes(fileSizeInBytes)
            .withPartition(spec.fields().size() == 0 ? null : currentKey) // set null if unpartitioned
            .withMetrics(metrics)
            .withSplitOffsets(splitOffsets)
            .build();
        completedFiles.add(dataFile);
      }

      this.currentFile = null;
    }
  }

  private static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] files;

    TaskCommit() {
      this.files = new DataFile[0];
    }

    TaskCommit(DataFile file) {
      this.files = new DataFile[] { file };
    }

    TaskCommit(List<DataFile> files) {
      this.files = files.toArray(new DataFile[0]);
    }

    DataFile[] files() {
      return files;
    }
  }
}
