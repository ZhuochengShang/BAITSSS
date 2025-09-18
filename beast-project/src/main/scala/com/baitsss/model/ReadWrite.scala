package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.listDateFolders
import com.baitsss.model.PipelineLandsatETModelNP.{enforcePartitionKV, extractMonthDay}
import edu.ucr.cs.bdlab.beast.RasterReadMixinFunctions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterFileRDD, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf}

import java.sql.Timestamp
import java.time.LocalDate

object ReadWrite {
  def main(args: Array[String]): Unit = {
    // --- Spark session initialization ---
    val conf = new SparkConf().setAppName("MultiTemporalDataAggregator")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val listener = new StageCountListener()
    sc.addSparkListener(listener)
    val listener2 = new StageCountListener()
    sc.addSparkListener(listener2)

    val startTime = System.nanoTime()
    // --- Define input directories ---
    val output = "read_write_test.tif"
    val input = "baitsss_output/nldas-upsampling-one-hour-nn.la.tif"
    val NLDAS:RDD[ITile[Array[Float]]] = sc.geoTiff(input)
    val part = new HashPartitioner(144)
    val NLDAS_part = enforcePartitionKV(NLDAS,part)
    GeoTiffWriter.saveAsGeoTiff(NLDAS_part.values, output, Seq(GeoTiffWriter.WriteMode -> "compatibility",GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW, GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    val endTime = System.nanoTime()
    println(s"Total stages executed: ${listener.stageCount}")
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
    //println(s"Metadata after reshape: ${upsampling_metadata}")
    spark.stop()
  }
}
