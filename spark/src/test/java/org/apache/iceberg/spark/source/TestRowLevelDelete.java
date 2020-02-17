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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.Or;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestRowLevelDelete {
  private static final Namespace namespace = Namespace.of("ns1", "ns2");
  private static final Joiner DOT = Joiner.on(".");
  private static final String SPARK_SQL_CATALOG_NAME = "spark.sql.iceberg.catalog";
  private static final String TABLE_NAME = "test_table";

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  private static SparkSession spark;
  private static String dbTable;

  private static Catalog catalog = null;

  @BeforeClass
  public static void start() throws IOException  {
    // we need to clear the previous session first
    try {
      spark = SparkSession.active();
      spark.stop();
    } catch (IllegalStateException e) {
      spark = null;
    }

    String sparkWarehouse = createTempDirectory("spark-warehouse-",
        asFileAttribute(fromString("rwxrwxrwx"))).toFile().getAbsolutePath();

    spark = SparkSession.builder()
        .master("local[2]")
        .config(SPARK_SQL_CATALOG_NAME, "hadoop")
        .config(DOT.join(SPARK_SQL_CATALOG_NAME, "warehouse"), sparkWarehouse)
        .getOrCreate();
    catalog = new HadoopCatalog(spark.sparkContext().hadoopConfiguration(), sparkWarehouse);
    dbTable = Joiner.on('/').join(sparkWarehouse, Joiner.on('/').join(namespace.levels()), TABLE_NAME);
  }

  @AfterClass
  public static void stop() {
    catalog = null;

    if (spark != null) {
      spark.stop();
    }
    spark = null;
  }

  @After
  public void clear() {
    catalog.dropTable(TableIdentifier.of(namespace, TABLE_NAME), true);
  }

  @Test
  public void testDeleteFromUnpartitionedTable() {
    TableIdentifier identifier = TableIdentifier.of(namespace, TABLE_NAME);
    catalog.createTable(identifier, SCHEMA, PartitionSpec.unpartitioned());
    // Create
    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );
    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF.select("id", "data").write().format("iceberg").mode("append").save(dbTable);
    verify(create);

    IcebergSource icebergTable = new IcebergSource();
    icebergTable.deleteWhere(dbTable, new Filter[]{new EqualTo("data", "a")});
    verify(Lists.newArrayList(
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    icebergTable.deleteWhere(dbTable, new Filter[]{new EqualTo("id", 2)});
    verify(Lists.newArrayList(
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    // delete with In expression
    icebergTable.deleteWhere(dbTable, new Filter[]{new In("data", new String[]{"c","d"})});
    verify(Lists.newArrayList(
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    // delete with Or expression
    icebergTable.deleteWhere(dbTable,
        new Filter[]{new Or(new EqualTo("id", 5), new EqualTo("data", "f"))});
    verify(Lists.newArrayList(
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    // delete with GreatThan expression
    icebergTable.deleteWhere(dbTable,
        new Filter[]{new GreaterThan("id", 7)});
    verify(Lists.newArrayList(new SimpleRecord(7, "g")));

    // delete with empty expression
    icebergTable.deleteWhere(dbTable,
        new Filter[]{});
    verify(Lists.newArrayList());
  }


  @Test
  public void testDeleteFromPartitionedTable() {
    TableIdentifier identifier = TableIdentifier.of(namespace, TABLE_NAME);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    catalog.createTable(identifier, SCHEMA, spec);

    // Create
    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2")
    );
    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF.select("id", "data").write().format("iceberg").mode("append").save(dbTable);
    verify(create);

    IcebergSource icebergTable = new IcebergSource();
    icebergTable.deleteWhere(dbTable, new Filter[]{new EqualTo("data", "a1")});
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete one row with partition column
    icebergTable.deleteWhere(dbTable, new EqualTo[]{new EqualTo("id", 1)});
    verify(Lists.newArrayList(
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete multiple rows with partition column
    icebergTable.deleteWhere(dbTable, new EqualTo[]{new EqualTo("id", 2)});
    verify(Lists.newArrayList(
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete with In expression
    icebergTable.deleteWhere(dbTable, new Filter[]{new In("data", new String[]{"c1", "c2"})});
    verify(Lists.newArrayList(
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete with Or expression
    icebergTable.deleteWhere(dbTable,
        new Filter[]{new Or(new EqualTo("id", 4), new EqualTo("data", "e1"))});
    verify(Lists.newArrayList(
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete with GreatThan expression
    icebergTable.deleteWhere(dbTable,
        new Filter[]{new GreaterThan("id", 5)});
    verify(Lists.newArrayList(new SimpleRecord(5, "e2")));

    // delete with empty expression
    icebergTable.deleteWhere(dbTable, new Filter[]{});
    verify(Lists.newArrayList());
  }

  private void verify(List<SimpleRecord> expected) {
    List<String> actual = spark.read().format("iceberg").load(dbTable).select("data").orderBy("id")
        .collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    Assert.assertEquals(expected.stream().map(SimpleRecord::getData).collect(Collectors.toList()), actual);
  }
}
