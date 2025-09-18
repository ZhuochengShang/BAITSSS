package com.baitsss.model

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.slf4j.LoggerFactory

import java.awt.geom.Point2D
import java.io.File
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}


object ReshapeCompareNN {

  def Raster_metadata(inputMetedata: RasterMetadata, allMetadata: Array[RasterMetadata], targetCRS: CoordinateReferenceSystem): RasterMetadata = {
    val initialMetadata = inputMetedata
    println("Initial metadata: " + initialMetadata)
    var minX1 = Double.MaxValue
    var minY1 = Double.MaxValue

    var maxX2 = Double.MinValue
    var maxY2 = Double.MinValue

    var minCellsizeX = Double.MaxValue
    var minCellsizeY = Double.MaxValue

    var rasterWidth = 0
    var rasterHeight = 0
    allMetadata.foreach(originMetadata => {
      val sourceCRS = CRSServer.sridToCRS(originMetadata.srid)

      val x1 = originMetadata.x1
      val y1 = originMetadata.y1
      val x2 = originMetadata.x2
      val y2 = originMetadata.y2
      val corners = Array[Double](x1, y1,
        x2, y1,
        x2, y2,
        x1, y2)
      originMetadata.g2m.transform(corners, 0, corners, 0, 4)

      val transform: TransformationInfo = findTransformationInfo(sourceCRS, targetCRS)
      transform.mathTransform.transform(corners, 0, corners, 0, 4)

      minX1 = minX1 min corners(0) min corners(2) min corners(4) min corners(6)
      maxX2 = maxX2 max corners(0) max corners(2) max corners(4) max corners(6)
      minY1 = minY1 min corners(1) min corners(3) min corners(5) min corners(7)
      maxY2 = maxY2 max corners(1) max corners(3) max corners(5) max corners(7)

      val col = originMetadata.rasterWidth
      val row = originMetadata.rasterHeight

      minCellsizeX = minCellsizeX min ((maxX2 - minX1).abs / col)
      minCellsizeY = minCellsizeY min ((maxY2 - minY1).abs / row)
    })

    rasterWidth = Math.floor(Math.abs(maxX2 - minX1) / minCellsizeX).toInt
    rasterHeight = Math.floor(Math.abs(maxY2 - minY1) / minCellsizeY).toInt

    val targetMetadataTmp = RasterMetadata.create(minX1, maxY2, maxX2, minY1, 4326, rasterWidth, rasterHeight, 256, 256)
    targetMetadataTmp
  }

  def main(args: Array[String]): Unit = {
    // --- Spark session initialization ---
    val conf = new SparkConf().setAppName("MultiTemporalDataAggregator")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val listener = new StageCountListener()
    sc.addSparkListener(listener)

    val startTime = System.nanoTime()

    // --- Define input directories ---
    val output = "baitsss_output"
    val targetCRS = CRS.decode("EPSG:4326") //4326, 3857

    // Define start and end dates.
    val Planet_daily = "Planet_CA_4B_BAITSSS/2023-01-01"
    val Planet_RDD: RasterRDD[Array[Int]] = sc.geoTiff(Planet_daily)
    val PlanetFloat: RasterRDD[Array[Float]] = RasterOperationsLocal.mapPixels(Planet_RDD, (p: Array[Int]) => {
      val value = ((p(1) - p(0)).toFloat / (p(1) + p(0)).toFloat).toFloat
      var pixelValue = value
      if (value > 1) {
        pixelValue = 1
      } else if (value < -1) {
        pixelValue = -1
      }
      val LAI = new CalculationLAI().CalculateLAI(p(1), p(0))
      Array(pixelValue, LAI.toFloat)
    })
    val Planet_metadata: RasterMetadata = PlanetFloat.first().rasterMetadata
    val Planet_all_metadata: Array[RasterMetadata] = RasterMetadata.allMetadata(PlanetFloat)
    val Planet_RDD_Metadata: RasterMetadata = Raster_metadata(Planet_metadata, Planet_all_metadata, targetCRS)
    val Planet_RDD_reshape: RasterRDD[Array[Float]] = RasterOperationsFocal.reshapeNN(PlanetFloat, RasterMetadata => Planet_RDD_Metadata)
    val output_file = s"$output/planet-reshape-one-day-nn.tif"
    GeoTiffWriter.saveAsGeoTiff(Planet_RDD_reshape, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_DEFLATE, GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    val endTime = System.nanoTime()
    println(s"Total stages executed: ${listener.stageCount}")
    println("Total time of whole process : Reshape NN rasterRDD: " + (endTime - startTime) / 1E9)
    println(s"Metadata after reshape: ${Planet_RDD_Metadata}")
    val minX1 = Planet_RDD_Metadata.x1
    val maxX2 = Planet_RDD_Metadata.x2
    val maxY2 = Planet_RDD_Metadata.y2
    val minY1 = Planet_RDD_Metadata.y1
    val rasterWidth = Planet_RDD_Metadata.rasterWidth
    val rasterHeight = Planet_RDD_Metadata.rasterHeight
    println(s"Metadata after reshape: ${minX1}, ${maxY2}, ${maxX2}, ${minY1}, ${rasterWidth}, ${rasterHeight}")
    spark.stop()
  }
}
