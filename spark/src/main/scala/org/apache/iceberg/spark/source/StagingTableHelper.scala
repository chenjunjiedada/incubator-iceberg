package org.apache.iceberg.spark.source


import com.google.common.base.Joiner
import com.google.common.collect.Maps
import java.util.UUID
import org.apache.iceberg.{DataFile, Table}
import org.apache.iceberg.TableProperties.WRITE_NEW_DATA_LOCATION
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.{HiveCatalog, HiveCatalogs}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

object StagingTableHelper {

  def parseCatalog() : Catalog = {
    val catalog = SparkSession.active.conf.get("spark.sql.iceberg.catalog")
    catalog match {
      case "hive" => HiveCatalogs.loadCatalog(SparkSession.active.sparkContext.hadoopConfiguration)
      case "hadoop" =>
        val location = SparkSession.active.conf.get("spark.sql.iceberg.catalog.warehouse")
        new HadoopCatalog(SparkSession.active.sparkContext.hadoopConfiguration, location)
      case _ => throw new IllegalArgumentException(s"Unsupported catalog type: " + catalog)
    }
  }

  def buildDeletionFiles(table: SparkTable, updatedRDD: RDD[InternalRow]): Iterable[DataFile] = {
    val baseTableIdentifier = buildTableIdentifier(table)
    val stagingUniquer = Joiner.on("_").join(System.currentTimeMillis().toString,
      UUID.randomUUID().toString.replaceAll("-", "_"))

    val icebergCatalog = parseCatalog()

    val stagingTableName = if (icebergCatalog.isInstanceOf[HiveCatalog]) {
      Joiner.on("_").join(baseTableIdentifier.name(), stagingUniquer)
    } else {
      stagingUniquer
    }

    val stagingTableIdentifier = buildStagingTableIdentity(icebergCatalog,
      baseTableIdentifier, stagingTableName)

    // Create the staging table
    val stagingTable = icebergCatalog.createTable(stagingTableIdentifier, table.metaSchema(),
      table.getIcebergTable.spec(),
      buildStagingProperties(table, icebergCatalog, stagingUniquer))
    val tablePath = getTablePath(icebergCatalog, stagingTable, stagingTableIdentifier)

    // Write into staging table, so as to generate updated data files
    val method = SparkSession.active.getClass.getDeclaredMethod("internalCreateDataFrame",
      classOf[RDD[InternalRow]], classOf[StructType], classOf[Boolean])
    method.setAccessible(true)
    method.invoke(SparkSession.active, updatedRDD,
      SparkSchemaUtil.convert(table.schema()), java.lang.Boolean.FALSE)
      .asInstanceOf[DataFrame]
      .select( s"_metadata_file_path", s"_metadata_row_id")
      .sort(s"_metadata_file_path")
      .write.format("iceberg").mode("append").save(tablePath)

    stagingTable.refresh()
    val snapshot = stagingTable.currentSnapshot()
    val files = snapshot.addedFiles()

    // Drop staging table
    icebergCatalog.dropTable(stagingTableIdentifier, false)

    // Return updated data files
    files.asScala
  }

  def buildTableIdentifier(table: SparkTable) : TableIdentifier =  {
    if (table.name().contains("/")) {
      val warehouse = SparkSession.active.conf.get("spark.sql.iceberg.catalog.warehouse")
      val tableName = table.name().substring(warehouse.length + 1)
      val levels = tableName.split("/").toSeq
      TableIdentifier.of(Namespace.of(levels.init:_*), levels.last)
    } else {
      val splits = table.name().split("\\.")
      val len = splits.length
      (Namespace.of(splits(len - 2)), splits(len - 1))
      TableIdentifier.of(Namespace.of(splits(len - 2)), splits(len - 1))
    }
  }

  def buildStagingTableIdentity(icebergCatalog: Catalog, tableIdentifier: TableIdentifier,
                                stagingTableName: String)
  : TableIdentifier = {
    if (icebergCatalog.isInstanceOf[HiveCatalog]) {
      TableIdentifier.of(tableIdentifier.namespace(), stagingTableName)
    } else {
      val levels = tableIdentifier.namespace().levels().toSeq :+ tableIdentifier.name()
      TableIdentifier.of(Namespace.of(levels:_*), stagingTableName)
    }
  }

  def buildStagingProperties(table: SparkTable, icebergCatalog: Catalog, stagingUniquer: String)
  : java.util.Map[String, String] = {
    val properties = Maps.newHashMap[String, String]()
    properties.putAll(table.getIcebergTable.properties())

    // Manipulate "location" for HiveCatalog only.
    // HadoopCatalog is not allowed to do that, but namespace could be used instead.
    if (icebergCatalog.isInstanceOf[HiveCatalog]) {
      val location = table.getIcebergTable.location()
      properties.put("location", location + "/" + stagingUniquer)
    }
    // Separate metadata and data locations of staging table,
    // so as to put its data files (added by "delete" operation) into the data location of the original table
    properties.put(WRITE_NEW_DATA_LOCATION, table.getIcebergTable.locationProvider().newDataLocation(""))

    properties
  }

  def getTablePath(catalog: Catalog, table: Table, tableIdentifier: TableIdentifier) : String = {
    if (catalog.isInstanceOf[HadoopCatalog]) {
      table.location()
    } else {
      Joiner.on(".").join(tableIdentifier.namespace().toString, tableIdentifier.name())
    }
  }
}
