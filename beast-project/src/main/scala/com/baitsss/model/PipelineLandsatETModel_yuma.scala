package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.{findLandsatFilesForTwoMonthWindowPaths, listDateFolders}
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.slf4j.LoggerFactory

import java.awt.geom.Point2D
import java.io.File
import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

object PipelineLandsatETModel_yuma {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val mdFmt = DateTimeFormatter.ofPattern("MM-dd")
  private val checkpointInterval = 2 //4 //6 // checkpoint every 4 hours to truncate lineage
  private val starttime = System.nanoTime()

  // Add this function to analyze RDD content
  def Raster_metadata(inputMetedata: RasterMetadata, allMetadata: Array[RasterMetadata], targetCRS: CoordinateReferenceSystem): (RasterMetadata, Double, Double, Double, Double) = {
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
    (targetMetadataTmp, minX1, maxX2, minY1, maxY2)
  }

  def AOI_metadata(AOI: RDD[IFeature], targetMetadata: (RasterMetadata, Double, Double, Double, Double)): RasterMetadata = {
    val corner1: Point2D.Double = new Point2D.Double
    val corner2: Point2D.Double = new Point2D.Double

    val geom = Reprojector.reprojectGeometry(AOI.first().getGeometry, targetMetadata._1.srid)
    val lineMBR: Envelope = geom.getEnvelopeInternal
    targetMetadata._1.modelToGrid(lineMBR.getMinX, lineMBR.getMinY, corner1)
    targetMetadata._1.modelToGrid(lineMBR.getMaxX, lineMBR.getMaxY, corner2)
    val i1 = Math.max(0, Math.min(corner1.x, corner2.x))
    val i2 = Math.min(targetMetadata._1.rasterWidth, Math.ceil(Math.max(corner1.x, corner2.x)))
    val j1 = Math.max(0, Math.min(corner1.y, corner2.y))
    val j2 = Math.min(targetMetadata._1.rasterHeight, Math.ceil(Math.max(corner1.y, corner2.y)))

    targetMetadata._1.gridToModel(i1, j1, corner1)
    targetMetadata._1.gridToModel(i2, j2, corner2)
    val x1 = Math.min(corner1.x, corner2.x)
    val y1 = Math.min(corner1.y, corner2.y)
    val x2 = Math.max(corner1.x, corner2.x)
    val y2 = Math.max(corner1.y, corner2.y)

    val minX = Math.max(lineMBR.getMinX, targetMetadata._2)
    val minY = Math.max(lineMBR.getMinY, targetMetadata._4)
    val maxX = Math.min(lineMBR.getMaxX, targetMetadata._3)
    val maxY = Math.min(lineMBR.getMaxY, targetMetadata._5)

    val cellSizeX = targetMetadata._1.getPixelScaleX
    val rasterW = Math.floor((x2 - x1) / cellSizeX).toInt
    val rasterH = Math.floor((y2 - y1) / cellSizeX).toInt

    val targetMetadataGeom = RasterMetadata.create(minX, maxY, maxX, minY, targetMetadata._1.srid, rasterW, rasterH, 256, 256)
    targetMetadataGeom
  }

  private def listLocalDateFolders(basePath: String): Array[(String, String)] = {
    val logger = LoggerFactory.getLogger(getClass)
    val dateRegex = raw"\d{4}-\d{2}-\d{2}".r
    val folderDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    try {
      // Remove "file://" prefix if present
      val cleanPath = basePath.replace("file://", "").replace("file:", "")
      val baseDir = new File(cleanPath)

      println(s"[DEBUG] listLocalDateFolders: checking $cleanPath")

      if (!baseDir.exists() || !baseDir.isDirectory) {
        logger.error(s"Base path does not exist or is not a directory: $cleanPath")
        return Array.empty[(String, String)]
      }

      val subDirs = baseDir.listFiles().filter(_.isDirectory).flatMap { dirStatus =>
        val folderName = dirStatus.getName.trim
        folderName match {
          case dateRegex() =>
            try {
              val parsedDate = LocalDate.parse(folderName, folderDateFormatter)
              val monthDay = parsedDate.format(DateTimeFormatter.ofPattern("MM-dd"))
              logger.info(s"Found date folder: $folderName -> MM-dd: $monthDay")
              // Return the full path including the folder name
              Some((monthDay, s"file://${dirStatus.getAbsolutePath}"))
            } catch {
              case e: Exception =>
                logger.warn(s"Error parsing folder name: $folderName", e)
                None
            }
          case _ =>
            logger.debug(s"Skipping non-date folder: $folderName")
            None
        }
      }

      println(s"[DEBUG] Found ${subDirs.length} date folders")
      subDirs
    } catch {
      case e: Exception =>
        logger.error(s"Error while listing folders under: $basePath", e)
        e.printStackTrace()
        Array.empty[(String, String)]
    }
  }

  def findLocalLandsatFiles(
                             targetDate: LocalDate,
                             baseFolder: String,
                             thresholdDays: Int
                           ): Map[LocalDate, Seq[String]] = {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val startDate = targetDate.minusMonths(1)
    val endDate = targetDate.plusMonths(1)
    val days = Iterator.iterate(startDate)(_.plusDays(1)).takeWhile(!_.isAfter(endDate)).toList

    val cleanPath = baseFolder.replace("file://", "").replace("file:", "")
    val baseDir = new File(cleanPath)

    val result = days.flatMap { date =>
      val folderPath = new File(baseDir, date.format(dateFormatter))
      if (folderPath.exists() && folderPath.isDirectory) {
        val tiffs = folderPath.listFiles()
          .filter(f => f.isFile && (f.getName.toLowerCase.endsWith(".tif") ||
            f.getName.toLowerCase.endsWith(".tiff")))
          .map(f => s"file://${f.getAbsolutePath}").toSeq

        if (tiffs.nonEmpty) {
          println(s"[DEBUG] Landsat for $date: found ${tiffs.length} files")
          Some(date -> tiffs)
        } else {
          None
        }
      } else {
        None
      }
    }.toMap

    println(s"[DEBUG] Total Landsat dates found: ${result.size}")
    if (result.size < thresholdDays) {
      println(s"[WARN] Only found ${result.size} days in window $startDate to $endDate")
    }
    result
  }

  def extractMonthDay(date: LocalDate): String = date.format(mdFmt)

  class StageCountingListener extends SparkListener {
    var stageCount = 0
    val hourlyCounts = scala.collection.mutable.ListBuffer.empty[(String, Int)]

    override def onStageCompleted(stage: SparkListenerStageCompleted): Unit = stageCount += 1

    def reset(): Unit = stageCount = 0

    def record(label: String): Unit = hourlyCounts += label -> stageCount
  }

  def estimateDAGDepth(rdd: RDD[_]): Int = {
    val indentLengths = rdd.toDebugString.linesIterator
      .map(_.takeWhile(_ == ' ').length)
      .toList
    if (indentLengths.nonEmpty)
      (indentLengths.max / 2) + 1
    else
      1
  }

  def enforcePartitionKV[T: ClassTag](
                                       rdd: RDD[ITile[T]],
                                       part: Partitioner,
                                       sort: Boolean = true
                                     ): RDD[(Int, ITile[T])] = {
    val kv = rdd.map(tile => (tile.tileID, tile))
    val partitioned = kv.partitioner match {
      case Some(p) if p == part => kv
      case _ => kv.partitionBy(part)
    }

    if (sort)
      partitioned.mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
    else
      partitioned
  }

  def processDay(
                  spark: SparkSession,
                  sc: SparkContext,
                  datein: LocalDate,
                  landsatDate: LocalDate,
                  metadata: RasterMetadata,
                  numParts: Int,
                  part: Partitioner,
                  prevOpt: Option[RDD[(Int, ITile[Array[Float]])]],
                  soilAWCKV: RDD[(Int, ITile[Float])],
                  soilFCKV: RDD[(Int, ITile[Float])],
                  nlcdKV: RDD[(Int, ITile[Float])],
                  elevKV: RDD[(Int, ITile[Float])],
                  stageCounter: Option[StageCountingListener] = None
                ): RDD[(Int, ITile[Array[Float]])] = {

    val dateStr = datein.toString
    println(s"\n[DEBUG] === Processing day $dateStr ===")

    try {

      // Metrics tracking
      case class HourlyMetrics(
                                hour: Int,
                                countBefore: Long,
                                countAfter: Long,
                                saveSize: Long,
                                saveSizeMB: Double,
                                saveTimeMs: Double,
                                loadTimeMs: Double,
                                processingTimeMs: Double,
                                dagDepthBefore: Int,
                                dagDepthAfter: Int,
                                stagesBeforeSave: Int,
                                stagesForSave: Int,
                                stagesForLoad: Int,
                                totalStages: Int,
                                sumBefore: Double,
                                sumAfter: Double
                              )

      val hourlyMetrics = scala.collection.mutable.ArrayBuffer[HourlyMetrics]()

      // Load PRISM data
      val prism_daily = "file:///Users/clockorangezoe/Desktop/BAITSSS_project/data/PRISM_2023_Geotiff/2023-08-10/PRISM_tmin_stable_4kmD2_20230810_bil.tif" // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
      val prismRetile = sc.geoTiff[Float](prism_daily).retile(20, 20)
      val targetMonthDay = extractMonthDay(datein)
      println(s"[DEBUG] Looking for month-day: $targetMonthDay")
      println(s"[DEBUG] Loading PRISM from: $prism_daily")
      val prismKV:RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(
          prismRetile, _ => metadata, numParts, part)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)


      // Add a pause after each hour
//      println(s"Pausing for 5 minutes after PRISM to allow UI inspection...")
//      println(s"You can examine the Spark UI at http://localhost:4040")
//      Thread.sleep(300000) // 5 minutes

      // Load Landsat data
      val b4Map = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/minnesota/minnesota/landsat_b4.tif"
      val b5Map = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/minnesota/minnesota/landsat_b5.tif"

      val b4RDD = sc.geoTiff[Float](b4Map)
      val b5RDD = sc.geoTiff[Float](b5Map)
      val rs4: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(
        b4RDD, _ => metadata, numParts, part)
      val rs5: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(
        b5RDD, _ => metadata, numParts, part)
      val lsOv: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.overlayArrayStreamsF(rs4, rs5)
      val landsKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(lsOv, (p:Array[Float]) => {
        val ndvi = (p(1) - p(0)).toFloat / (p(1) + p(0)).toFloat
        var pixelValue = ndvi
        if (ndvi > 1) {
          pixelValue = 1
        } else if (ndvi < -1) {
          pixelValue = -1
        }
        val lai = new CalculationLAI().ndviLaiFunc(ndvi).toFloat
        //val lai = new CalculationLAI().CalculateLAI(p(1),p(0)).toFloat
        Array(pixelValue, lai)
      })
      //val landsKV = enforcePartitionKV(lsOv_notnull_val,part)
      // Create base static layer
      val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
      val baseKV_full: RDD[(Int,ITile[Array[Float]])] =
        RasterOperationsLocal
          .overlayArrayStreamsF2(baseKV, landsKV)

      // Process hourly data
      val NLDAS_filter = listDateFolders(new Path("file:///Users/clockorangezoe/Desktop/BAITSSS_project/data/NLDAS_2023_Geotiff"), FileSystem.get(sc.hadoopConfiguration))
      val maybeFolderNLDAS: Option[String] = NLDAS_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      val NLDAS_file = maybeFolderNLDAS.getOrElse {
        throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
      }
      val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray.slice(12,17)

      var cycle: RDD[(Int,ITile[Array[Float]])] = prevOpt.orNull
      for ((path, idx) <- NLDAS_RasterRDD.zipWithIndex) {
        val hourStartTime = System.nanoTime()
        stageCounter.foreach(_.reset())
        println(s"\n=== Hour ${idx + 1} Processing ===")
        println(s"[DEBUG] Processing file: $path")

        try {
          // Load hourly data
          val hrRDD: RDD[ITile[Array[Float]]] = sc.geoTiff[Array[Float]](path._2)
          println(s"Partition count: ${hrRDD.getNumPartitions}")
          val hrKV_filter: RDD[ITile[Array[Float]]] = RasterOperationsLocal.mapPixels(hrRDD, (p: Array[Float]) => Array(
            p(0), // Air Temp
            p(1), // Surface Humidity
            Math.sqrt(p(3) * p(3) + p(4) * p(4)).toFloat, // Wind Speed
            p(5) // Shortwave
          ))
          val hrRDD_part = hrKV_filter.retile(10, 10)
          val hrKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsFocal.reshapeNN2(hrRDD_part, _ => metadata, numParts, part)
          //val hrKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsFocal.reshapeBilinear(hrRDD_part, _ => metadata, part)
          // Overlay operation
          val layered = if (cycle == null) {
            println("[DEBUG] First hour - no previous cycle")
            RasterOperationsLocal.overlayArrayStreamsF2(baseKV, hrKV, landsKV)
          } else {
            println("[DEBUG] Continuing from previous cycle")
            RasterOperationsLocal.overlayArrayStreamsF2(baseKV, hrKV, landsKV, cycle)
          }

          // Calculate next iteration
          val rawNext = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](BandCalculationCheck2.IterativeCalculationPre2(layered, null), part)
          if (rawNext == null) {
            println("[ERROR] BandCalculationCheck2 returned null")
          }

          // Calculate sum for data integrity check
          val sumBefore = 0
          val sumAfter = 0

          // Update cycle for next iteration
          // Unpersist the intermediate RDDs created in this iteration
          hrRDD.unpersist()
          hrKV.unpersist()
          hrKV_filter.unpersist()
          layered.unpersist()

          // Optionally unpersist previous cycle if no longer needed
          if (cycle != null) cycle.unpersist(blocking = false)
          cycle = rawNext //.persist(StorageLevel.MEMORY_AND_DISK_SER)

          // Add a pause after each hour
          //          println(s"Pausing for 5 minutes after Hour $idx to allow UI inspection...")
          //          println(s"You can examine the Spark UI at http://localhost:4040")
          //          Thread.sleep(300000) // 5 minutes

          // Record metrics
          val processingTime = (System.nanoTime() - hourStartTime) / 1e6

          hourlyMetrics += HourlyMetrics(
            hour = idx + 1,
            countBefore = 0,
            countAfter = 0,
            saveSize = 0,
            saveSizeMB = 0,
            saveTimeMs = 0,
            loadTimeMs = 0,
            processingTimeMs = processingTime,
            dagDepthBefore = 0,
            dagDepthAfter = 0,
            stagesBeforeSave = 0,
            stagesForSave = 0,
            stagesForLoad = 0,
            totalStages = 0,
            sumBefore = sumBefore,
            sumAfter = sumAfter
          )

          println(f"✅ Hour ${idx + 1} completed in $processingTime%.2f ms")

        } catch {
          case e: Exception =>
            println(s"[ERROR] Failed processing hour ${idx + 1}: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      // Clean up base RDD after all hours are processed
      prismKV.unpersist()
      baseKV.unpersist()
      baseKV_full.unpersist()
      // Return the final cycle
      cycle

    } catch {
      case e: Exception =>
        println(s"[ERROR] processDay failed: ${e.getMessage}")
        e.printStackTrace()
        prevOpt.orNull
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MultiTemporalET").setIfMissing("spark.master", "local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //conf.set("spark.cleaner.referenceTracking.blocking", "true")
    // conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    // conf.set("spark.cleaner.ttl", "3600")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val startTime = System.nanoTime()
    // Set checkpoint directory to local filesystem

    val stageCounter = new StageCountingListener
    sc.addSparkListener(stageCounter)

    // Load AOI from local filesystem
    //val aoi = sc.shapefile("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/yuma/YumaAgMain_latlon3.shp") // Update this path
    //val b4Map = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/landsat8/LC08_L1TP_038037_20190829_20190903_01_T1_B5_4326.tif"
    val samplePaths = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/minnesota/minnesota/landsat_b4.tif"//b4Map
    val sample = sc.geoTiff[Int](samplePaths).retile(256,256)
    val initial = sample.first().rasterMetadata
    val globalMeta = initial //Raster_metadata(initial, RasterMetadata.allMetadata(sample), CRS.decode("EPSG:4326"))
    val aoiMeta = globalMeta //AOI_metadata(aoi, globalMeta)
    val parts = sample.getNumPartitions
    val part = new HashPartitioner(parts)

    // Load elevation, soil, and NLCD data from local filesystem
    val elevRDD = sc.geoTiff[Float]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/NLDAS_elevation.tif") // Update
    val awcRDD = sc.geoTiff[Float]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/inputdata/soil_awc") // Update
    val fcRDD = sc.geoTiff[Float]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/inputdata/soil_fc") // Update
    val nlcdInt = sc.geoTiff[Int]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/nlcd32_4326_LZW.tif") // Update

    val nlcdRDD = nlcdInt.mapPixels(_.toFloat)
    val elevKV:RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(elevRDD, _ => aoiMeta, parts, part).persist(StorageLevel.DISK_ONLY)
    val awcKV:RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(awcRDD, _ => aoiMeta,  parts, part).persist(StorageLevel.DISK_ONLY)
    val fcKV:RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(fcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.DISK_ONLY)
    val nlcdKV:RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => aoiMeta, parts, part).persist(StorageLevel.DISK_ONLY)

    // Add a pause after each hour
//    println(s"Pausing for 5 minutes after reshape YEARLY to allow UI inspection...")
//    println(s"You can examine the Spark UI at http://localhost:4040")
//    Thread.sleep(300000) // 5 minutes

    var curr = LocalDate.parse("2023-08-10")
    val end = LocalDate.parse("2023-08-10")
    var currLandsat = LocalDate.parse("2023-01-01")
    val endDate = end.toString
    var star = curr.toString
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]]
    = None
    while (!curr.isAfter(end)) {
      println(s"\n=== [Day ${curr}] Processing ===")
      stageCounter.reset()

      val daily = processDay(
        spark, sc, curr, currLandsat, aoiMeta, parts, part,
        prevOpt, awcKV, fcKV, nlcdKV, elevKV
      ) //.persist(StorageLevel.DISK_ONLY)

      //daily.count() // force materialization
      println(s"[Stage Count] Day $curr completed in ${stageCounter.stageCount} stages")
      println(s"[DEBUG] Final RDD DAG:\n${daily.toDebugString}")

      prevOpt.foreach(_.unpersist(true))
      prevOpt = Some(daily)

      curr = curr.plusDays(1)
      currLandsat = currLandsat.plusDays(1)

      // Optional: JVM Heap snapshot
      val mb = 1024 * 1024
      val runtime = Runtime.getRuntime
      println(f"[Memory] Used: ${(runtime.totalMemory - runtime.freeMemory) / mb} MB, Total: ${runtime.totalMemory / mb} MB")
    }

    val finalRdd: RasterRDD[Array[Float]] = prevOpt.get.values.mapPixels(arr => {
      val out = Array.fill(5)(-9999f)
      Array.copy(arr, 0, out, 0, math.min(arr.length, 5))
      out
    })
//    val finalRdd2: RasterRDD[Float] = finalRdd.mapPixels(arr => {
//      arr(0)
//    })
    val output = "file:///Users/clockorangezoe/Desktop/BAITSSS_project"
    val output_file = s"$output/landsat-$star-$endDate.mn.nn.tif"
    GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(
      GeoTiffWriter.WriteMode -> "compatibility",
      GeoTiffWriter.OutputNumBands -> "5",
      GeoTiffWriter.BigTiff -> "yes",
      GeoTiffWriter.FillValue -> "-9999"
    ))
    val endTime = System.nanoTime()
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
    spark.stop()
  }
}
