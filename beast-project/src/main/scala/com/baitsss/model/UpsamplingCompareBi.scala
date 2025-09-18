package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.{findLandsatFilesForTwoMonthWindowPaths, listDateFolders}
import com.baitsss.model.PipelineLandsatETModelNP.{enforcePartitionKV, extractMonthDay}
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}

import java.sql.Timestamp
import java.time.LocalDate
import scala.util.Random

object UpsamplingCompareBi {
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
    val output = "baitsss_output"
     //val elevRDD = sc.geoTiff[Float]("NLDAS_Elevation_2023_Geotiff")
    //val awcRDD = sc.geoTiff[Float]("Soil_2023/awc_gNATSGO_US.tif")
    //val prism_daily = sc.geoTiff[Float]("PRISM_2023_Geotiff/2023-01-01") // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
    // target bounding box and size
        val minX = -125//-93.9715407457249512
        val maxY = 53 //45.4296143523275759
        val maxX = -67 //-93.7725996921366232
        val minY = 25 //45.3259506562490628
//        val minX = -118.4799183376948690 //-124.57118886084827
//        val maxY =  34.4936496252132798 //42.05537666170133
//        val maxX = -117.8205067999291202 //-113.87223419721632
//        val minY =  33.5954066392961153 //32.43618917424716

//        val minX = -124.57118886084827
//        val maxY = 42.05537666170133
//        val maxX = -113.87223419721632
//        val minY = 32.43618917424716


    //    val rasterWidth  = 2024//31698
    //    val rasterHeight = 2756//28498

//    val rasterWidth = 31698
//    val rasterHeight = 28498
    val rasterWidth = 167200 // ~58° longitude
    val rasterHeight = 103900 // ~28° latitude


    //    val rasterWidth = 31698
    //    val rasterHeight = 28498

    // create a 256×256-tiled, EPSG:4326 metadata object
    val targetMetadata = RasterMetadata.create(
      minX, maxY,
      maxX, minY,
      4326,             // CRS EPSG code
      rasterWidth,
      rasterHeight,
      256,              // tile width
      256               // tile height
    )


    var curr = LocalDate.parse("2023-01-01")
    val end = LocalDate.parse("2023-01-01")
    var currLandsat = LocalDate.parse("2021-01-01")
    val part = new HashPartitioner(144)
    val targetMonthDay = extractMonthDay(curr)

    val NLDAS_filter = listDateFolders(new Path("NLDAS_2023_Geotiff"), FileSystem.get(sc.hadoopConfiguration))
    val maybeFolderNLDAS: Option[String] = NLDAS_filter
      .find { case (monthDay, _) => monthDay == targetMonthDay }
      .map(_._2)

    val NLDAS_file = maybeFolderNLDAS.getOrElse {
      throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
    }
    val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray.slice(1,2)
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]] = None
    var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull

    for ((path, idx) <- NLDAS_RasterRDD.zipWithIndex) {
      val hourStartTime = System.nanoTime()

      println(s"\n=== Hour ${idx + 1} Processing ===")
      println(s"[DEBUG] Processing file: $path")
      val hrRDD: RDD[ITile[Array[Float]]] = sc.geoTiff[Array[Float]](path._2)
      val hrKV_filter: RDD[ITile[Array[Float]]] = RasterOperationsLocal.mapPixels(hrRDD, (p: Array[Float]) => {
        // Define safe getter with default
        def get(idx: Int): Float = if (p.length > idx && !p(idx).isNaN) p(idx) else 0.0f
        Array(
          (get(0)), // res(0)
          (get(1)), // res(1)
          (Math.sqrt(get(3) * get(3) + get(4) * get(4)).toFloat), // res(2) - wind speed
          (get(5)) // res(3)
        )
      })
      val hrRDD_part:RDD[ITile[Array[Float]]] = hrKV_filter.retile(10, 10)
      //val hrKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsFocal.reshapeNN2(hrRDD_part, _ => targetMetadata, 144, part)
      val hrKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsFocal.reshapeBilinear(hrRDD_part, _ => targetMetadata, part)
      // Overlay operation
      val layered = if (cycle == null) {
        println("[DEBUG] First hour - no previous cycle")
        hrKV
      } else {
        println("[DEBUG] Continuing from previous cycle")
        RasterOperationsLocal.overlayArrayStreamsF2(hrKV,cycle)
      }
      cycle = layered//RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(layered, (value: Array[Float]) => {
        // Make sure `value.length >= 4` (otherwise indexing 0..3 is unsafe)
//        val out = Array.fill(5)(1.5f)
//        if(value != null) out(0) = value(0)
////        out(0) = value(3) + value(2)
////        out(1) = value(2) + value(1)
////        out(2) = value(2) + value(value.length - 1) // <-- use length-1 for last index
////        out(3) = value(0)
////        out(4) = value(1)
//        out
//      })
    }
    //cycle.count()
    val output_file = "aoidata_rdpro_landsat_ca/nldas-upsampling-one-hour-bi.tif"
    cycle.foreach(tile => {
      tile._2.getPixelValue(tile._2.x1, tile._2.y1)
    })
    //GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.OutputNumBands -> "5",GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW, GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    val endTime = System.nanoTime()
    println(s"Total stages executed: ${listener.stageCount}")
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
    //println(s"Metadata after reshape: ${upsampling_metadata}")
    spark.stop()
  }
}
