package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.{enforcePartitionKV, extractMonthDay, findLandsatFilesForTwoMonthWindowPaths, listDateFolders, mdFmt}
import com.baitsss.model.PipelineLandsatETModel_listFile.getClass
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.util.Parallel2
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, MapPixelsTile, RasterOperationsFocal, RasterOperationsLocal, StackedTile}
import org.apache.commons.math3.util.FastMath.hypot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.slf4j.LoggerFactory

import java.awt.geom.Point2D
import java.io.File
import java.net.URI
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object PipelineLandsatETModel2 {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val mdFmt = DateTimeFormatter.ofPattern("MM-dd")
  private val checkpointInterval = 2 //4 //6 // checkpoint every 4 hours to truncate lineage
  private val starttime = System.nanoTime()

  // Add this function to analyze RDD content
  def analyzeRDDContent(rdd: RDD[(Int, ITile[Array[Float]])], label: String): Unit = {
    println(s"\n[DEBUG] === Analyzing $label RDD Content ===")

    // Sample a representative tile
    val sample = rdd.first()
    val (key, tile) = sample

    // Basic information
    println(s"[DEBUG] Key type: ${key.getClass.getName}, Value: $key")
    println(s"[DEBUG] Tile type: ${tile.getClass.getName}")
    println(s"[DEBUG] Tile dimensions: ${tile.tileWidth}x${tile.tileHeight}")

    // Check pixel content in multiple locations
//    val locations = Seq((0, 0), (10, 10), (tile.tileWidth / 2, tile.tileHeight / 2))
//    locations.foreach { case (x, y) =>
//      if (x < tile.tileWidth && y < tile.tileHeight) {
//        val pixel = tile.getPixelValue(x, y)
//        println(s"[DEBUG] Pixel at ($x,$y): type=${pixel.getClass.getName}, length=${pixel.length}")
//        println(s"[DEBUG] Values: [${pixel.take(10).mkString(", ")}${if (pixel.length > 10) "..." else ""}]")
//      }
//    }

    // Analyze the RDD's lineage
    println(s"[DEBUG] RDD ID: ${rdd.id}")
    println(s"[DEBUG] Number of partitions: ${rdd.getNumPartitions}")
    println(s"[DEBUG] Dependencies: ${rdd.dependencies.size}")
    rdd.dependencies.foreach { dep =>
      println(s"[DEBUG]   - ${dep.getClass.getSimpleName} → RDD ${dep.rdd.id}")
    }

    // Look at Spark's debug string
    val debugString = rdd.toDebugString
    val debugLines = debugString.split("\n")
    println(s"[DEBUG] RDD structure (first 5 lines of ${debugLines.length}):")
    debugLines.take(5).foreach(line => println(s"[DEBUG]   $line"))

    // Estimate size
    val count = rdd.count()
    val avgRecordSize = tile.tileWidth * tile.tileHeight * tile.getPixelValue(0, 0).length * 4 // 4 bytes per float
    val estimatedSizeGB = (count * avgRecordSize) / (1024.0 * 1024.0 * 1024.0)
    println(s"[DEBUG] Records: $count")
    println(s"[DEBUG] Estimated size: $estimatedSizeGB GB")

    // Check for internal references that might cause size growth
    try {
      val fields = tile.getClass.getDeclaredFields
      println(s"[DEBUG] Tile internal fields: ${fields.length}")
      fields.take(10).foreach { field =>
        field.setAccessible(true)
        val fieldType = field.getType.getSimpleName
        val fieldName = field.getName
        println(s"[DEBUG]   - $fieldName: $fieldType")
      }
    } catch {
      case e: Exception =>
        println(s"[DEBUG] Could not inspect internal fields: ${e.getMessage}")
    }
  }
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
      val prism_daily = "PRISM_2023_Geotiff"
      val targetMonthDay = extractMonthDay(datein)
      println(s"[DEBUG] Looking for month-day: $targetMonthDay")
      println(s"[DEBUG] Loading PRISM from: $prism_daily")
      val prismKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(
        sc.geoTiff[Float](prism_daily), _ => metadata, numParts), part)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      prismKV.count()

      // Load Landsat data
      val b4Map = findLocalLandsatFiles(
        landsatDate,
        "/Users/clockorangezoe/Desktop/BAITSSS_project/data/California_Landsat8_4B_2021/B4",
        10
      )
      val b5Map = findLocalLandsatFiles(
        landsatDate,
        "/Users/clockorangezoe/Desktop/BAITSSS_project/data/California_Landsat8_4B_2021/B5",
        10
      )

      if (b4Map.isEmpty || b5Map.isEmpty) {
        println(s"[ERROR] No Landsat data found for $landsatDate")
        return prevOpt.orNull
      }

      val b4RDD = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
      val b5RDD = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
      val rs4 = RasterOperationsFocal.reshapeNN(b4RDD, _ => metadata, numParts)
      val rs5 = RasterOperationsFocal.reshapeNN(b5RDD, _ => metadata, numParts)
      val lsOv: RasterRDD[Array[Int]] = RasterOperationsLocal.overlay(rs4, rs5)
      val lsOv_notnull_val: RasterRDD[Array[Float]] = lsOv.mapPixels(p => {
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
      val landsKV = enforcePartitionKV(lsOv_notnull_val, part, sort = true)

      // Create base static layer
      val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
      val baseKV_full: RDD[(Int, ITile[Array[Float]])] =
        RasterOperationsLocal
          .overlayArrayStreamsF2(baseKV, landsKV)
          .persist(StorageLevel.DISK_ONLY)
      baseKV_full.count()

      // Clean up intermediate RDDs
      baseKV.unpersist(true)
      prismKV.unpersist(true)
      landsKV.unpersist(true)

      // Process hourly data
      val dayDir = new File(s"aoidata_nldas_all_reshape/$dateStr")
      println(s"[DEBUG] Checking NLDAS path: ${dayDir.getAbsolutePath}")
      println(s"[DEBUG] NLDAS exists: ${dayDir.exists()}, isDir: ${dayDir.isDirectory}")

      // In processDay function, update the hourly data loading section:

      // Replace your current tifPaths code with this:
      val tifPaths = if (dayDir.exists() && dayDir.isDirectory) {
        // Look for TIF files in all sub-directories
        def findAllTifFiles(dir: File): Array[String] = {
          val result = new ArrayBuffer[String]()

          if (dir.exists() && dir.isDirectory) {
            dir.listFiles().foreach { file =>
              if (file.isFile && file.getName.toLowerCase.endsWith(".tif")) {
                result += s"file://${file.getAbsolutePath}"
              } else if (file.isDirectory) {
                result ++= findAllTifFiles(file)
              }
            }
          }

          result.toArray
        }

        val allTifFiles = findAllTifFiles(dayDir).sorted
        println(s"[DEBUG] Found ${allTifFiles.length} TIF files")
        allTifFiles.foreach(path => println(s"[DEBUG] TIF: $path"))
        allTifFiles
      } else {
        println(s"[DEBUG] No NLDAS directory found")
        Array.empty[String]
      }

      if (tifPaths.isEmpty) {
        println(s"[WARN] No hourly TIF files found for $dateStr")
        return prevOpt.orNull
      }

      println(s"[DEBUG] Processing ${tifPaths.length} hourly files")

      var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull
      println(s"[DEBUG] Initial cycle is null: ${cycle == null}")

      for ((path, idx) <- tifPaths.zipWithIndex) {
        val hourStartTime = System.nanoTime()
        stageCounter.foreach(_.reset())
        val stagesAtStart = stageCounter.map(_.stageCount).getOrElse(0)

        println(s"\n=== Hour ${idx + 1} Processing ===")
        println(s"[DEBUG] Processing file: $path")

        try {
          // Load hourly data
          val hrRDD: RDD[ITile[Array[Float]]] = sc.geoTiff[Array[Float]](path)
          val hrKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(hrRDD, _ => metadata, numParts), part)
          // Add before and after size estimates
          if (cycle != null) {
            println("[DEBUG] Creating simplified cycle to reduce data size")

            // Log memory before transformation
            val runtime = Runtime.getRuntime
            val mb = 1024 * 1024
            println(f"[Memory] Before: Used=${(runtime.totalMemory - runtime.freeMemory) / mb}MB, Free=${runtime.freeMemory / mb}MB")

            // Sample a tile to understand the data
            val sampleTile = cycle.first()._2
            val samplePixel = sampleTile.getPixelValue(sampleTile.x1, sampleTile.y1)
            println(s"[DEBUG] Before truncation - sample bands: ${samplePixel.length}")

            // Get existing partitioner
            val existingPartitioner = cycle.partitioner

            // The key is to create completely new arrays, not views of existing arrays
            val keptValues = 5

            // Create a new RDD with completely new data arrays
            cycle = cycle.mapPartitions(iter => {
              iter.map { case (key, tile) =>
                // Use MapPixelsTile but with a function that creates completely new arrays
                val newTile = new MapPixelsTile[Array[Float], Array[Float]](tile, pixel => {
                  // Important: Create a completely new array with exactly keptValues length
                  val truncated = new Array[Float](keptValues)
                  // Only copy up to keptValues elements
                  val copyCount = Math.min(keptValues, pixel.length)
                  System.arraycopy(pixel, 0, truncated, 0, copyCount)
                  // Return the fixed-size array (this is key for memory reduction)
                  truncated
                })

                (key, newTile)
              }
            }, preservesPartitioning = true)

            // Force immediate evaluation to materialize the changes
            cycle.cache() // Cache to prevent recomputation
            val countAfter = cycle.count()

            // Sample again to verify the transformation worked
            val sampleAfterTile = cycle.first()._2
            val sampleAfterPixel = sampleAfterTile.getPixelValue(sampleTile.x1, sampleTile.y1)
            println(s"[DEBUG] After truncation - sample bands: ${sampleAfterPixel.length}")

            // Log memory after transformation
            println(f"[Memory] After: Used=${(runtime.totalMemory - runtime.freeMemory) / mb}MB, Free=${runtime.freeMemory / mb}MB")

            // Verify partitioner was preserved
            println(s"[DEBUG] Original partitioner: ${existingPartitioner}, " +
              s"New partitioner: ${cycle.partitioner}")

            // If partitioner was lost, reapply it
            if (!cycle.partitioner.isDefined && existingPartitioner.isDefined) {
              cycle = cycle.partitionBy(existingPartitioner.get)
              cycle.cache()
              cycle.count()
            }

            // Hint to garbage collector to clean up now
            System.gc()
          }

          // Overlay operation
          val layered = if (cycle == null) {
            println("[DEBUG] First hour - no previous cycle")
            RasterOperationsLocal.overlayArrayStreamsF2(baseKV_full, hrKV)
          } else {
            println("[DEBUG] Continuing from previous cycle")
            RasterOperationsLocal.overlayArrayStreamsF2(baseKV_full, hrKV, cycle)
          }

          // Calculate next iteration
          val rawNext = BandCalculationCheck2.IterativeCalculationPre2(layered, null)
          if (rawNext == null) {
            println("[ERROR] BandCalculationCheck2 returned null")
          }

          // Count stages before save
          val countBefore = rawNext.count()
          val stagesBeforeSave = stageCounter.map(_.stageCount).getOrElse(0) - stagesAtStart

          // Track data before saving
          val dagDepthBefore = estimateDAGDepth(rawNext)

          println(s"  Before save:")
          println(s"    Records: $countBefore")
          println(s"    DAG depth: $dagDepthBefore")
          println(s"    Stages executed: $stagesBeforeSave")

          // Calculate sum for data integrity check
          val sumBefore = 0
//          rawNext.map { case (k, tile) =>
//            val pixelCount = tile.tileWidth * tile.tileHeight
//            val bytesPerPixel = 20 // 5 bands * 4 bytes per float
//            val tileSize = pixelCount * bytesPerPixel
//            tileSize.toLong
//          }.reduce(_ + _)

          // Save to local filesystem
          val checkpointPath = s"$localBase/day-$dateStr-hour-$idx"

          val stagesBeforeSaveOp = stageCounter.map(_.stageCount).getOrElse(0)
          val saveStart = System.nanoTime()
          rawNext.saveAsObjectFile(checkpointPath)
          val saveTime = (System.nanoTime() - saveStart) / 1e6
          val stagesForSave = stageCounter.map(_.stageCount).getOrElse(0) - stagesBeforeSaveOp

          // Check saved size
          val savedDir = new File(checkpointPath.replace("file://", ""))
          val savedSize = if (savedDir.exists()) {
            savedDir.listFiles().map(_.length()).sum
          } else {
            0L
          }
          val savedSizeMB = savedSize / (1024.0 * 1024.0)

          println(f"  Save operation:")
          println(f"    Size: ${savedSize}%,d bytes (${savedSizeMB}%.2f MB)")
          println(f"    Time: ${saveTime}%.2f ms")
          println(f"    Stages: $stagesForSave")

          // Delete previous checkpoint if exists
          if (idx > 0) {
            val prevPath = s"$localBase/day-$dateStr-hour-${idx - 1}"
            val prevDir = new File(prevPath.replace("file://", ""))
            if (prevDir.exists()) {
              val prevSize = prevDir.listFiles().map(_.length()).sum
              prevDir.listFiles().foreach(_.delete())
              prevDir.delete()
              println(f"    Deleted previous: ${prevSize}%,d bytes")
            }
          }


          // Load from local filesystem
          val stagesBeforeLoad = stageCounter.map(_.stageCount).getOrElse(0)
          val loadStart = System.nanoTime()
          val loaded = sc.objectFile[(Int, ITile[Array[Float]])](checkpointPath)

          // Create a mapped RDD to force new lineage
          val mapped = loaded.map(x => x)

          // Repartition and persist the loaded RDD
          val fresh = mapped
            .partitionBy(part)
            .persist(StorageLevel.DISK_ONLY)

          val countAfter = fresh.count()
          val loadTime = (System.nanoTime() - loadStart) / 1e6
          val stagesForLoad = stageCounter.map(_.stageCount).getOrElse(0) - stagesBeforeLoad

          // Track data after loading
          val dagDepthAfter = estimateDAGDepth(fresh)

          println(f"  Load operation:")
          println(f"    Records: $countAfter")
          println(f"    Time: ${loadTime}%.2f ms")
          println(f"    Stages: $stagesForLoad")

          // Calculate sum after reload
          val sumAfter = 0
          //          fresh.map { case (k, tile) =>
//            val pixelCount = tile.tileWidth * tile.tileHeight
//            val bytesPerPixel = 20
//            val tileSize = pixelCount * bytesPerPixel
//            tileSize.toLong
//          }.reduce(_ + _)

          println(s"  After reload:")
          println(s"    DAG depth: $dagDepthAfter")

          // Data integrity check
          if (countBefore != countAfter) {
            println(s"  ⚠️ WARNING: Count mismatch - before: $countBefore, after: $countAfter")
          }

//          if (Math.abs(sumBefore - sumAfter) > 0.001) {
//            println(f"  ⚠️ WARNING: Sum mismatch - before: $sumBefore%.6f, after: $sumAfter%.6f")
//          } else {
//            println(f"  ✅ Data integrity verified - sum: $sumBefore%.6f")
//          }

          // Show lineage truncation result
          if (dagDepthAfter < dagDepthBefore / 2) {
            println(s"  ✅ Lineage successfully truncated: $dagDepthBefore -> $dagDepthAfter")
          } else {
            println(s"  ⚠️ Partial lineage truncation: $dagDepthBefore -> $dagDepthAfter")
          }

          // Clean up old RDDs
          if (cycle != null) cycle.unpersist(true)
          layered.unpersist(true)
          rawNext.unpersist(true)
          hrKV.unpersist(true)

          // Update cycle for next iteration
          cycle = fresh

          // Record metrics
          val processingTime = (System.nanoTime() - hourStartTime) / 1e6
          val totalStages = stageCounter.map(_.stageCount).getOrElse(0) - stagesAtStart

          hourlyMetrics += HourlyMetrics(
            hour = idx + 1,
            countBefore = countBefore,
            countAfter = countAfter,
            saveSize = savedSize,
            saveSizeMB = savedSizeMB,
            saveTimeMs = saveTime,
            loadTimeMs = loadTime,
            processingTimeMs = processingTime,
            dagDepthBefore = dagDepthBefore,
            dagDepthAfter = dagDepthAfter,
            stagesBeforeSave = stagesBeforeSave,
            stagesForSave = stagesForSave,
            stagesForLoad = stagesForLoad,
            totalStages = totalStages,
            sumBefore = sumBefore,
            sumAfter = sumAfter
          )

          stageCounter.foreach(sc => {
            sc.record(s"${datein}_hr${idx + 1}")
            println(s"[Stage Count] Total stages for hour ${idx + 1}: ${sc.stageCount}")
          })

          println(f"✅ Hour ${idx + 1} completed in $processingTime%.2f ms")

        } catch {
          case e: Exception =>
            println(s"[ERROR] Failed processing hour ${idx + 1}: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      // Print summary if we have metrics
      if (hourlyMetrics.nonEmpty) {
        println("\n=== Hourly Processing Summary ===")
        println("Hour | Records | Save (MB) | Stages (Proc/Save/Load/Total) | DAG Depth | Time (ms)")
        println("-----|---------|-----------|-------------------------------|-----------|----------")

        hourlyMetrics.foreach { m =>
          println(f"${m.hour}%4d | ${m.countAfter}%7d | ${m.saveSizeMB}%9.2f | ${m.stagesBeforeSave}%4d/${m.stagesForSave}%4d/${m.stagesForLoad}%4d/${m.totalStages}%5d | ${m.dagDepthBefore}%4d -> ${m.dagDepthAfter}%4d | ${m.processingTimeMs}%8.0f")
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
    val aoi = sc.shapefile("file:///home/zshan011/CA/CA.shp") // Update this path
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
    val elevKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(elevRDD, _ => aoiMeta, parts), part).persist(StorageLevel.DISK_ONLY)
    elevKV.foreachPartition(_ => ())
    val awcKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(awcRDD, _ => aoiMeta, parts), part).persist(StorageLevel.DISK_ONLY)
    awcKV.foreachPartition(_ => ())
    val fcKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(fcRDD, _ => aoiMeta, parts), part).persist(StorageLevel.DISK_ONLY)
    fcKV.foreachPartition(_ => ())
    val nlcdKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(nlcdRDD, _ => aoiMeta, parts), part).persist(StorageLevel.DISK_ONLY)
    nlcdKV.foreachPartition(_ => ())

    var curr = LocalDate.parse("2023-01-01")
    val end = LocalDate.parse("2023-01-01")
    var currLandsat = LocalDate.parse("2021-01-01")
    val endDate = end.toString
    var star = curr.toString
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]]
    = None
    while (!curr.isAfter(end)) {
      println(s"\n=== [Day ${curr}] Processing ===")
      stageCounter.reset()

      val daily = processDay(
        spark, sc, curr, currLandsat, aoi, aoiMeta, parts, part,
        prevOpt, awcKV, fcKV, nlcdKV, elevKV
      )//.persist(StorageLevel.DISK_ONLY)

      daily.count() // force materialization
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
    val output = "file:///home/zshan011/aoidata_rdpro_landsat_ca"
    val output_file = s"$output/landsat-$star-$endDate.nn.tif"
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
