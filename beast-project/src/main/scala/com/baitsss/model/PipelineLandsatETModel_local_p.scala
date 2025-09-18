package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.{enforcePartitionKV, findLandsatFilesForTwoMonthWindowPaths, listDateFolders}
import com.baitsss.model.PipelineLandsatETModelNP.extractMonthDay
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, MapPixelsTile, RasterOperationsFocal, RasterOperationsLocal}
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
import scala.io.StdIn

object PipelineLandsatETModel_local_p {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val mdFmt = DateTimeFormatter.ofPattern("MM-dd")
  private val checkpointInterval = 2 //4 //6 // checkpoint every 4 hours to truncate lineage
  private val starttime = System.nanoTime()
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

  def processDay(
                  spark: SparkSession,
                  sc: SparkContext,
                  datein: LocalDate,
                  landsatDate: LocalDate,
                  aoi: RDD[IFeature],
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
      // Define paths
      val localPath = "/home/zshan011/workflow-data"
      val localBase = s"file://$localPath"

      // Create local directory if it doesn't exist
      val localDir = new File(localPath)
      if (!localDir.exists()) {
        localDir.mkdirs()
      }

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
      val targetMonthDay = extractMonthDay(datein)
      val prism_daily = "PRISM_2023_Geotiff" // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
      val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration)) //ListDate_daily_hourly(prism_daily)
      val maybeFolderPrism: Option[String] = PRISM_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)
      val prismKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(
          sc.geoTiff[Float](maybeFolderPrism.get), _ => metadata, numParts, part)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Load Landsat data
      val b4Map = findLocalLandsatFiles(
        landsatDate,
        "California_Landsat8_4B_2021/B4",
        7
      )
      val b5Map = findLocalLandsatFiles(
        landsatDate,
        "California_Landsat8_4B_2021/B5",
        7
      )

      if (b4Map.isEmpty || b5Map.isEmpty) {
        println(s"[ERROR] No Landsat data found for $landsatDate")
        return prevOpt.orNull
      }

      val b4RDD = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
      val b5RDD = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
      val rs4: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b4RDD, _ => metadata, numParts, part)
      val rs5: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b5RDD, _ => metadata, numParts, part)
      val lsOv: RDD[(Int, ITile[Array[Int]])] = RasterOperationsLocal.overlayArrayStreamsF(rs4, rs5)
      val landsKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(lsOv, (p: Array[Int]) => {
        try {
          if (p.length < 2) {
            Array(0.001f, 0.001f)
          } else {
            val red = p(0).toFloat
            val nir = p(1).toFloat

            if (red < 0 || nir < 0 || (red == 0 && nir == 0)) {
              Array(0.001f, 0.001f)
            } else {
              val ndvi = (nir - red) / (nir + red + 0.000001f)
              val lai = try {
                new CalculationLAI().ndviLaiFunc(ndvi)
              } catch {
                case _: Exception => -9999f
              }
              Array(ndvi, lai)
            }
          }
        } catch {
          case e: Exception =>
            println(s"[ERROR] Failed to process pixel: ${e.getMessage}")
            Array(-9999f, -9999f)
        }
      })

      val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
      val baseKV_full: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.overlayArrayStreamsF2(baseKV, landsKV).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val checkpointPath_base = s"$localBase/day-$dateStr-base"
//      baseKV_full_ck.saveAsObjectFile(checkpointPath_base)
//      val baseKV_full = sc.objectFile[(Int, ITile[Array[Float]])](checkpointPath_base).partitionBy(part)
//        .persist(StorageLevel.DISK_ONLY)
//      baseKV_full_ck.unpersist(true)

      // Clean up intermediate RDDs
//      baseKV.unpersist(true)
//      prismKV.unpersist(true)
//      landsKV.unpersist(true)

      // Process hourly data
      val dayDir = new File(s"NLDAS_2023_Geotiff/$dateStr")
      println(s"[DEBUG] Checking NLDAS path: ${dayDir.getAbsolutePath}")
      println(s"[DEBUG] NLDAS exists: ${dayDir.exists()}, isDir: ${dayDir.isDirectory}")

      // In processDay function, update the hourly data loading section:

      // Replace your current tifPaths code with this:
      val NLDAS_filter = listDateFolders(new Path("NLDAS_2023_Geotiff"), FileSystem.get(sc.hadoopConfiguration))
      val maybeFolderNLDAS: Option[String] = NLDAS_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      val NLDAS_file = maybeFolderNLDAS.getOrElse {
        throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
      }
      val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray

      var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull
      println(s"[DEBUG] Initial cycle is null: ${cycle == null}")

      for ((path, idx) <- NLDAS_RasterRDD.zipWithIndex) {
        val hourStartTime = System.nanoTime()
        println(s"\n=== Hour ${idx + 1} Processing ===")
        println(s"[DEBUG] Processing file: $path")

        try {
          // Load hourly data
          val hrRDD: RDD[ITile[Array[Float]]] = sc.geoTiff[Array[Float]](path._2)
          val hrKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsFocal.reshapeNN2(hrRDD, _ => metadata, numParts, part)
          val hrKV_filter: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(hrKV, (p:Array[Float]) => Array(
            p(0), // Air Temp
            p(1), // Surface Humidity
            Math.sqrt(p(3) * p(3) + p(4) * p(4)).toFloat, // Wind Speed
            p(5) // Shortwave
          ))

          // Overlay operation
          val layered = if (cycle == null) {
            println("[DEBUG] First hour - no previous cycle")
            RasterOperationsLocal.overlayArrayStreamsF2(baseKV_full, hrKV_filter)
          } else {
            println("[DEBUG] Continuing from previous cycle")
            RasterOperationsLocal.overlayArrayStreamsF2(baseKV_full, hrKV_filter, cycle)
          }

          // Calculate next iteration
          val rawNext = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](BandCalculationCheck2.IterativeCalculationPre2(layered, null), part)
          if (rawNext == null) {
            println("[ERROR] BandCalculationCheck2 returned null")
          }
          // Calculate sum for data integrity check
          val sumBefore = 0

          // Save to local filesystem
//          val checkpointPath = s"$localBase/day-$dateStr-hour-$idx"
//          val saveStart = System.nanoTime()
//          rawNext.saveAsObjectFile(checkpointPath)
//          rawNext.unpersist(true)
//          System.gc()
//          val saveTime = (System.nanoTime() - saveStart) / 1e6

          // Check saved size
//          val savedDir = new File(checkpointPath.replace("file://", ""))
//          val savedSize = if (savedDir.exists()) {
//            savedDir.listFiles().map(_.length()).sum
//          } else {
//            0L
//          }
//          val savedSizeMB = savedSize / (1024.0 * 1024.0)

//          println(f"  Save operation:")
//          println(f"    Size: ${savedSize}%,d bytes (${savedSizeMB}%.2f MB)")
//          println(f"    Time: ${saveTime}%.2f ms")

          // Delete previous checkpoint if exists
//          if (idx > 0) {
//            val prevPath = s"$localBase/day-$dateStr-hour-${idx - 1}"
//            val prevDir = new File(prevPath.replace("file://", ""))
//            if (prevDir.exists()) {
//              val prevSize = prevDir.listFiles().map(_.length()).sum
//              prevDir.listFiles().foreach(_.delete())
//              prevDir.delete()
//              println(f"    Deleted previous: ${prevSize}%,d bytes")
//            }
//          }

//          val loadStart = System.nanoTime()
//          val loaded = sc.objectFile[(Int, ITile[Array[Float]])](checkpointPath)

          // Create a mapped RDD to force new lineage
          // Repartition and persist the loaded RDD //can be removed
//          val fresh = loaded
//            .partitionBy(part)
//            .persist(StorageLevel.DISK_ONLY)
//
//          // Clean up old RDDs
//          if (cycle != null) cycle.unpersist(true)
//          layered.unpersist(true)
//          rawNext.unpersist(true)
//          hrKV.unpersist(true)

          // Update cycle for next iteration
          cycle = rawNext

          // Add a pause after each hour
          //          println(s"Pausing for 5 minutes after Hour $idx to allow UI inspection...")
          //          println(s"You can examine the Spark UI at http://localhost:4040")
          //          Thread.sleep(300000) // 5 minutes
          // Record metrics
          val processingTime = (System.nanoTime() - hourStartTime) / 1e6
          println(f"✅ Hour ${idx + 1} completed in $processingTime%.2f ms")

        } catch {
          case e: Exception =>
            println(s"[ERROR] Failed processing hour ${idx + 1}: ${e.getMessage}")
            e.printStackTrace()
        }
      }
      // Clean up base RDD after all hours are processed
      baseKV_full.unpersist(true)
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
    val aoi = sc.shapefile("file:///home/zshan011/riverside/POLYGON.shp") // Update this path
    val startDate = LocalDate.parse("2021-01-01")
    val b4SampleMap = findLandsatFilesForTwoMonthWindowPaths(
      startDate,
      "California_Landsat8_4B_2021/B4",
      thresholdDays = 7,
      sc.hadoopConfiguration
    )
    val samplePaths = b4SampleMap.values.flatten.mkString(" ")
    if (samplePaths.isEmpty) throw new IllegalStateException("No Landsat B4 files found in two-month window around " + startDate)
    val sample = sc.geoTiff[Int](samplePaths)
    val initial = sample.first().rasterMetadata
    val globalMeta = Raster_metadata(initial, RasterMetadata.allMetadata(sample), CRS.decode("EPSG:4326"))
    val aoiMeta = AOI_metadata(aoi, globalMeta)
    val parts = sample.getNumPartitions
    val part = new HashPartitioner(parts)

    // Load elevation, soil, and NLCD data from local filesystem
    val elevRDD = sc.geoTiff[Float]("file:///home/zshan011/NLDAS_Elevation_2023_Geotiff")  // Update
    val awcRDD = sc.geoTiff[Float]("file:///home/zshan011/Soil_2023/awc_gNATSGO_US.tif")  // Update
    val fcRDD = sc.geoTiff[Float]("file:///home/zshan011/Soil_2023/fc_gNATSGO_US.tif")  // Update
    val nlcdInt = sc.geoTiff[Int]("file:///home/zshan011/NLCD_2023")  // Update

    val nlcdRDD = nlcdInt.mapPixels(_.toFloat)
    val elevKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(elevRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_ONLY)
    val awcKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(awcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_ONLY)
    val fcKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(fcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_ONLY)
    val nlcdKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_ONLY)

    var curr = LocalDate.parse("2023-01-01")
    val end = LocalDate.parse("2023-01-01")
    var currLandsat = LocalDate.parse("2021-01-01")
    val endDate = end.toString
    var star = curr.toString
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]]
    = None
    while (!curr.isAfter(end)) {
      println(s"\n=== [Day ${curr}] Processing ===")

      val daily = processDay(
        spark, sc, curr, currLandsat, aoi, aoiMeta, parts, part,
        prevOpt, awcKV, fcKV, nlcdKV, elevKV
      )//.persist(StorageLevel.DISK_ONLY)

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
    val output = "file:///home/zshan011/aoidata_rdpro_landsat_ca"
    val output_file = s"$output/landsat-$star-$endDate.nn.tif"
    GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(
      GeoTiffWriter.WriteMode -> "compatibility",
      GeoTiffWriter.OutputNumBands -> "5",
      GeoTiffWriter.BigTiff -> "yes",
      GeoTiffWriter.FillValue -> "-9999"
    ))
    val endTime = System.nanoTime()
//    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
//    println(s"You can examine the Spark UI at http://localhost:4040")
//    Thread.sleep(2000000) // 5 minutes
    StdIn.readLine()
    //System.end.
    spark.stop()
  }
}
