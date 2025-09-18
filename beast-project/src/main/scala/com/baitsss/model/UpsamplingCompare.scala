package com.baitsss.model

import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, ReshapeTile}
import org.apache.spark.SparkConf
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.awt.geom.Point2D
import java.io.File
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

object UpsamplingCompare {
  def main(args: Array[String]): Unit = {
    // --- Spark session initialization ---
    val conf = new SparkConf().setAppName("MultiTemporalDataAggregator")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    val listener = new ShuffleMemoryListener(sc)
    sc.addSparkListener(listener)
    val listener2 = new StageCountListener()
    sc.addSparkListener(listener2)

    val startTime = System.nanoTime()

    // --- Define input directories ---
    val input = "file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/minnesota/minnesota/landsat_b4.tif"
    val ndvi:RDD[ITile[Array[Float]]] =sc.geoTiff(input)
    val targetMetadata = ndvi.first().rasterMetadata
    val NLDAS = "file:///Users/clockorangezoe/Desktop/BAITSSS_project/data/NLDAS_2023_Geotiff/2023-08-10/NLDAS_FORA0125_H.A20230810.0000.002.tif"
    println(s"Loading raster from: $NLDAS")

    val NLDAS_RDD_curr: RDD[ITile[Array[Float]]] = sc.geoTiff(NLDAS)//.retile(10,10)
    // Extract values
    val NLDAS_obtain_values = NLDAS_RDD_curr.mapPixels(p => {
      val values = new Array[Float](4)
      values(0) = p(0) // Tair_oC
      values(1) = p(1) // S_hum
      //wind_array = np.sqrt(wind_u_array ** 2 + wind_v_array ** 2)
      val wind_array = Math.sqrt(p(3) * p(3) + p(4) * p(4)) //uz
      values(2) = wind_array.toFloat
      values(3) = p(5) // In_short
      values
    })//.persist(StorageLevel.MEMORY_AND_DISK)

    val reshape = RasterOperationsFocal.reshapeNN(NLDAS_obtain_values,RasterMetadata=>targetMetadata)
    val output = "file:///Users/clockorangezoe/Desktop/BAITSSS_project"
    val output_file = s"$output/nldas.mn.tif"
    val output_file2 = s"$output/nlcd.mn.tif"

    GeoTiffWriter.saveAsGeoTiff(reshape, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility",GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW, GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    spark.stop()
  }
}
// Custom Spark Listener to track shuffle and memory usage
class ShuffleMemoryListener(sc: org.apache.spark.SparkContext) extends SparkListener {
  var totalShuffleRead: Long = 0L
  var totalShuffleWrite: Long = 0L
  var maxMemoryUsed: Long = 0L

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val metrics = stageCompleted.stageInfo.taskMetrics

    // Track shuffle read bytes
    totalShuffleRead += Option(metrics.shuffleReadMetrics)
      .map(_.remoteBytesRead)
      .getOrElse(0L) / (1024 * 1024) // Convert to MB

    // Use `bytesWritten` to track shuffle writes
    val shuffleWriteSize = Option(metrics.shuffleWriteMetrics)
      .map(_.bytesWritten) // Compatible with Spark 3.1.2+
      .getOrElse(0L)
    totalShuffleWrite += shuffleWriteSize / (1024 * 1024)

    // Track peak memory usage (sum of all used memory)
    maxMemoryUsed = Math.max(maxMemoryUsed, getTotalMemoryUsed())
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logMemoryUsage()
  }

  /** Get total memory used across all executors */
  private def getTotalMemoryUsed(): Long = {
    sc.getExecutorMemoryStatus.map {
      case (_, (totalMem, remainingMem)) => (totalMem - remainingMem) / (1024 * 1024) // Convert to MB
    }.sum
  }

  /** Log memory usage after each job */
  private def logMemoryUsage(): Unit = {
    val executorMemoryStatus = sc.getExecutorMemoryStatus.map {
      case (executorId, (totalMem, remainingMem)) =>
        s"Executor: $executorId, Total: ${totalMem / (1024 * 1024)} MB, Used: ${(totalMem - remainingMem) / (1024 * 1024)} MB, Free: ${remainingMem / (1024 * 1024)} MB"
    }.mkString("\n")

    println(s"--- Memory Usage After Job ---\n$executorMemoryStatus\n")
  }
}