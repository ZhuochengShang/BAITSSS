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

object UpsamplingCompareBilinear {
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
    val minX = -118.4799183376948690 //-124.57118886084827
    val maxY =  34.4936496252132798 //42.05537666170133
    val maxX = -117.8205067999291202 //-113.87223419721632
    val minY =  33.5954066392961153 //32.43618917424716


    //    val rasterWidth  = 2024//31698
    //    val rasterHeight = 2756//28498
    val rasterWidth  = 7841//31698
    val rasterHeight = 7951//28498

    // create a 256×256-tiled, EPSG:4326 metadata object
    val targetMetadata2 = RasterMetadata.create(
      minX, maxY,
      maxX, minY,
      4326, // CRS EPSG code
      rasterWidth,
      rasterHeight,
      256, // tile width
      256 // tile height
    )


    // --- Define input directories ---
    val input = "file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/landsat-2023-01-01-2023-01-01.nn.tif/awc_gNATSGO_US_ca.tif"
    //"file:///Users/clockorangezoe/Downloads/yuma2_psscene_analytic_sr_udm2/composite4326.tif"
    val ndvi:RDD[ITile[Array[Float]]] =sc.geoTiff(input)
    val targetMetadata = ndvi.first().rasterMetadata
    val NLDAS = "file:///Users/clockorangezoe/Desktop/BAITSSS_project/data/NLDAS_2023_Geotiff/2023-08-10/NLDAS_FORA0125_H.A20230810.0100.002.tif"
    println(s"Loading raster from: $NLDAS")

    val NLDAS_RDD_curr: RDD[ITile[Array[Float]]] = sc.geoTiff(NLDAS)
      val NLDAS_retile = NLDAS_RDD_curr.retile(10,10)
    // Extract values
    val NLDAS_obtain_values = NLDAS_retile.mapPixels(p => {
      val values = new Array[Float](4)
      values(0) = p(0) // Tair_oC
      values(1) = p(1) // S_hum
      //wind_array = np.sqrt(wind_u_array ** 2 + wind_v_array ** 2)
      val wind_array = Math.sqrt(p(3) * p(3) + p(4) * p(4)) //uz
      values(2) = wind_array.toFloat
      values(3) = p(5) // In_short
      values
    })//.persist(StorageLevel.MEMORY_AND_DISK)

    val reshape:RDD[(Int,ITile[Array[Float]])] = RasterOperationsFocal.reshapeBilinear(NLDAS_obtain_values,RasterMetadata=>targetMetadata2)
    val output = "file:///Users/clockorangezoe/Desktop/BAITSSS_project"
    val output_file = s"$output/nldas.bi.mn.tif"
//    reshape.foreach(tile => {
//      tile._2.getPixelValue(tile._2.x1, tile._2.y1)
//    })

    GeoTiffWriter.saveAsGeoTiff(reshape.values, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility",GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW, GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    //GeoTiffWriter.saveAsGeoTiff(reshapeNLCD, output_file2, Seq(GeoTiffWriter.WriteMode -> "compatibility",GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW, GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    spark.stop()
  }
}
