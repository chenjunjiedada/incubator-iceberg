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

package org.apache.iceberg.spark.source

import org.apache.iceberg.{DataFiles, PartitionSpec}
import org.apache.iceberg.expressions.{Evaluator, Expressions}
import org.apache.iceberg.spark.{SparkFilters, SparkSchemaUtil}
import org.apache.iceberg.spark.source.Reader.StructLikeInternalRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.sources.Filter
import scala.collection.JavaConverters

class RowLevelDeleteSupport(table: SparkTable) {
  def deleteWhere(filters: Array[Filter]): Unit = {
    if (filters.length == 0) {
      table.getIcebergTable.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit()
    } else {
      val txn = table.getIcebergTable.newTransaction()

      //Step 1: generate delete files
      val deletionFiles = StagingTableHelper.buildDeletionFiles(table, buildDeleteRdd(filters)).toArray

      // Step 2: append delete files
      if (!deletionFiles.isEmpty) {
        val fastAppend = txn.newAppend();
        deletionFiles.map{file =>
          val deletionFile = DataFiles.builder(PartitionSpec.unpartitioned)
            .copy(file)
            .withDeletionType(1)
            .build()

          fastAppend.appendFile(deletionFile)
        }

        fastAppend.commit()
      }

      txn.commitTransaction()
    }
  }


  def buildDeleteRdd(filters: Array[Filter]): RDD[InternalRow] = {
    val sparkSession = SparkSession.active
    val schema = table.schema().asStruct()
    val structType = SparkSchemaUtil.convert(table.schema())
    val expression = filters.map(SparkFilters.convert).reduceLeft(Expressions.and)
    table.pushFilters(filters)

    val dataToUpdate = new DataSourceRDD(sparkSession.sparkContext,
      JavaConverters.asScalaBufferConverter(table.planInputPartitions).asScala
    )

    val updatedDataRdd = dataToUpdate.mapPartitions { p =>
      val evaluator = new Evaluator(schema, expression)
      p.filter { row =>
        val structLikeInternalRow = new StructLikeInternalRow(structType).setRow(row)
        evaluator.eval(structLikeInternalRow)
      }
    }

    updatedDataRdd
  }

}
