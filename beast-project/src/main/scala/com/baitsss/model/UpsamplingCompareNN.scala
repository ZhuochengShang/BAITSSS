package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.{findLandsatFilesForTwoMonthWindowPaths, listDateFolders}
import com.baitsss.model.PipelineLandsatETModelNP.extractMonthDay
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.LocalDate

object UpsamplingCompareNN {
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
    val minX = -118.4799183376948690 //-124.57118886084827
    val maxY =  34.4936496252132798 //42.05537666170133
    val maxX = -117.8205067999291202 //-113.87223419721632
    val minY =  33.5954066392961153 //32.43618917424716

//    val minX = -124.57118886084827
//    val maxY = 42.05537666170133
//    val maxX = -113.87223419721632
//    val minY = 32.43618917424716

//    val minX = -125//-93.9715407457249512
//    val maxY = 53 //45.4296143523275759
//    val maxX = -67 //-93.7725996921366232
//    val minY = 25 //45.3259506562490628

//    val minX = -124.7844079 //-93.9715407457249512
//    val maxY = 49.3457868 //45.4296143523275759
//    val maxX = -66.9513812 //-93.7725996921366232
//    val minY = 24.7433195 //45.3259506562490628

    val rasterWidth =  7841
    val rasterHeight =  7951
    //    val rasterWidth  = 2024//31698
//    val rasterHeight = 2756//28498

//    val rasterWidth = 31698
//    val rasterHeight = 28498

//    val rasterWidth = 167200 // ~58° longitude
//    val rasterHeight = 103900 // ~28° latitude

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

    var curr = LocalDate.parse("2023-08-10")
    val end = LocalDate.parse("2023-08-10")
    var currLandsat = LocalDate.parse("2021-01-01")
    val part = new HashPartitioner(144)
    val targetMonthDay = extractMonthDay(curr)


//    val elevRDD = sc.geoTiff[Float]("NLDAS_Elevation_2023_Geotiff").retile(10, 10)
//    val awcRDD = sc.geoTiff[Float]("Soil_2023/awc_gNATSGO_US.tif")
//    val fcRDD = sc.geoTiff[Float]("Soil_2023/fc_gNATSGO_US.tif")
//    val nlcdInt = sc.geoTiff[Int]("NLCD_2023")
//
//    val nlcdRDD = nlcdInt.mapPixels(_.toFloat)
////    val elevKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(elevRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
////    val soilAWCKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(awcRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
////    val soilFCKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(fcRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
////    val nlcdKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//    val elevKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(elevRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val soilAWCKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(awcRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val soilFCKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(fcRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val nlcdKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//    val prism_daily = "PRISM_2023_Geotiff" // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
//    val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration)) //ListDate_daily_hourly(prism_daily)
//    val maybeFolderPrism: Option[String] = PRISM_filter
//      .find { case (monthDay, _) => monthDay == targetMonthDay }
//      .map(_._2)
//    val prismRetile = sc.geoTiff[Float](maybeFolderPrism.get).retile(20, 20)
//    //val prismKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(prismRetile, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val prismKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(prismRetile, _ => targetMetadata, 144, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    // Add a pause after each hour
//    //      println(s"Pausing for 5 minutes after PRISM to allow UI inspection...")
//    //      println(s"You can examine the Spark UI at http://localhost:4040")
//    //      Thread.sleep(300000) // 5 minutes
//    // Landsat ±1m
//    val b4Map = findLandsatFilesForTwoMonthWindowPaths(currLandsat, "California_Landsat8_4B_2021/B4", 10, sc.hadoopConfiguration)
//    val b5Map = findLandsatFilesForTwoMonthWindowPaths(currLandsat, "California_Landsat8_4B_2021/B5", 10, sc.hadoopConfiguration)
//    val b4RDD: RasterRDD[Int] = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
//    val b5RDD: RasterRDD[Int] = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
//    val rs4: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b4RDD, _ => targetMetadata, 144, part)
//    val rs5: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b5RDD, _ => targetMetadata, 144, part)
//    val lsOv: RDD[(Int, ITile[Array[Int]])] = RasterOperationsLocal.overlayArrayStreamsF(rs4, rs5)
//    val landsKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(lsOv, (p: Array[Int]) => {
//      try {
//        if (p.length < 2) {
//          Array(0.001f, 0.001f)
//        } else {
//          val red = p(0).toFloat
//          val nir = p(1).toFloat
//
//          if (red < 0 || nir < 0 || (red == 0 && nir == 0)) {
//            Array(0.001f, 0.001f)
//          } else {
//            val ndvi = (nir - red) / (nir + red + 0.000001f)
//            val lai = try {
//              new CalculationLAI().ndviLaiFunc(ndvi)
//            } catch {
//              case _: Exception => -9999f
//            }
//            Array(ndvi, lai)
//          }
//        }
//      } catch {
//        case e: Exception =>
//          println(s"[ERROR] Failed to process pixel: ${e.getMessage}")
//          Array(-9999f, -9999f)
//      }
//    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val materize: RDD[(Int, ITile[Array[Float]])] =
//      RasterOperationsLocal
//        .overlayArrayStreamsF2(baseKV, landsKV) //materize.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val baseKV_full = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(materize, (temp: Array[Float]) => {
//      temp
//    }), part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    baseKV_full.count()

    val NLDAS_filter = listDateFolders(new Path("NLDAS_2023_Geotiff"), FileSystem.get(sc.hadoopConfiguration))
    val maybeFolderNLDAS: Option[String] = NLDAS_filter
      .find { case (monthDay, _) => monthDay == targetMonthDay }
      .map(_._2)

    val NLDAS_file = maybeFolderNLDAS.getOrElse {
      throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
    }
    val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray.slice(0,1)
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]] = None
    var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull

    for ((path, idx) <- NLDAS_RasterRDD.zipWithIndex) {
      val hourStartTime = System.nanoTime()

      println(s"\n=== Hour ${idx + 1} Processing ===")
      println(s"[DEBUG] Processing file: $path")
      val hrRDD: RDD[ITile[Array[Float]]] = sc.geoTiff[Array[Float]](path._2)
      val hrKV_filter: RDD[ITile[Array[Float]]] = RasterOperationsLocal.mapPixels(hrRDD, (p: Array[Float]) => Array(
        p(0), // Air Temp
        p(1), // Surface Humidity
        Math.sqrt(p(3) * p(3) + p(4) * p(4)).toFloat, // Wind Speed
        p(5), // Shortwave
        p(0)
      ))
      val hrRDD_part = hrKV_filter.retile(10, 10)
      //val hrKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsFocal.reshapeNN2(hrRDD_part, _ => targetMetadata, 144, part)
      val hrKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsFocal.reshapeNN2(hrRDD_part, _ => targetMetadata, 144, part)
      // Overlay operation
      val layered = if (cycle == null) {
        println("[DEBUG] First hour - no previous cycle")
        hrKV
      } else {
        println("[DEBUG] Continuing from previous cycle")
        RasterOperationsLocal.overlayArrayStreamsF2(hrKV, cycle)
      }
      cycle = layered
//        RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(layered, (value: Array[Float]) => {
//        // Make sure `value.length >= 4` (otherwise indexing 0..3 is unsafe)
//        val out = Array.fill(5)(-9999f)
//        out(0) = value(3) + value(2)
//        out(1) = value(2) + value(1)
//        out(2) = value(2) + value(value.length - 1) // <-- use length-1 for last index
//        out(3) = value(0)
//        out(4) = value(1)
//        out
//      })
    }
    //cycle.count()
    val output_file = s"$output/nldas-upsampling-one-hour-nn.la.tif"
//    cycle.foreach( tile => {
//      tile._2.getPixelValue(tile._2.x1,tile._2.y1)
//    })
val finalRdd: RasterRDD[Array[Float]] = cycle.values.mapPixels(arr => {
  val out = Array.fill(5)(0.1f)
  Array.copy(arr, 0, out, 0, math.min(arr.length, 5))
  out
})
    GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(
      GeoTiffWriter.WriteMode -> "compatibility",
      GeoTiffWriter.OutputNumBands -> "5",
      GeoTiffWriter.BigTiff -> "yes",
      GeoTiffWriter.FillValue -> "-9999"
    ))
    val endTime = System.nanoTime()
    println(s"Total stages executed: ${listener.stageCount}")
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
    //println(s"Metadata after reshape: ${upsampling_metadata}")
    spark.stop()
  }
}
