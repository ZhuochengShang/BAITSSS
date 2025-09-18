package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.{findLandsatFilesForTwoMonthWindowPaths, listDateFolders}
import com.baitsss.model.PipelineLandsatETModelNP_checkp.enforcePartitionKV
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.util.Parallel2
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.commons.math3.util.FastMath.hypot
import org.apache.hadoop.conf.Configuration
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
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.io.StdIn
import scala.reflect.ClassTag

object PipelineLandsatETModelNP {
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

    val targetMetadataGeom = RasterMetadata.create(minX, maxY, maxX, minY, targetMetadata._1.srid, rasterW, rasterH, 10, 10)
    targetMetadataGeom
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
                  aoi: RDD[IFeature],
                  metadata: RasterMetadata,
                  numParts: Int,
                  part: Partitioner,
                  prevOpt: Option[RDD[(Int, ITile[Array[Float]])]],
                  soilAWCKV: RDD[(Int, ITile[Float])],
                  soilFCKV: RDD[(Int, ITile[Float])],
                  nlcdKV: RDD[(Int, ITile[Float])],
                  elevKV: RDD[(Int, ITile[Float])],
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

      //val prismPath = listDateFolders(new Path("PRISM_2023_Geotiff"), fs).find(_._1==extractMonthDay(date)).get._2
      val startDaytime = System.nanoTime()
      val targetMonthDay = extractMonthDay(datein)
      val prism_daily = "PRISM_2023_Geotiff" // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
      val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration)) //ListDate_daily_hourly(prism_daily)
      val maybeFolderPrism: Option[String] = PRISM_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)
      val prismRetile = sc.geoTiff[Float](maybeFolderPrism.get).retile(20,20)
      //val prismKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(prismRetile, _ => metadata, numParts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val prismKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(prismRetile, _ => metadata, numParts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
      // Add a pause after each hour
      //      println(s"Pausing for 5 minutes after PRISM to allow UI inspection...")
      //      println(s"You can examine the Spark UI at http://localhost:4040")
      //      Thread.sleep(300000) // 5 minutes

      // Landsat ±1m
      val b4Map = findLandsatFilesForTwoMonthWindowPaths(landsatDate, "California_Landsat8_4B_2021/B4", 10, sc.hadoopConfiguration)
      val b5Map = findLandsatFilesForTwoMonthWindowPaths(landsatDate, "California_Landsat8_4B_2021/B5", 10, sc.hadoopConfiguration)
      val b4RDD: RasterRDD[Int] = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
      val b5RDD: RasterRDD[Int] = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
      val rs4: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b4RDD, _ => metadata, numParts, part)
      val rs5: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b5RDD, _ => metadata, numParts, part)
      val lsOv: RDD[(Int, ITile[Array[Int]])] = RasterOperationsLocal.overlayArrayStreamsF(rs4, rs5)
      val landsKV: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(lsOv, (p: Array[Int]) => {
        try {
          if (p.length < 2) {
            Array(-9999f, -9999f)
          } else {
            val red = p(0).toFloat
            val nir = p(1).toFloat
              val ndvi = (nir - red) / (nir + red + 0.000001f)
              val lai = try {
                new CalculationLAI().ndviLaiFunc(ndvi)
              } catch {
                case _: Exception => -9999f
              }
              Array(ndvi, lai)
          }
        } catch {
          case e: Exception =>
            println(s"[ERROR] Failed to process pixel: ${e.getMessage}")
            Array(-9999f, -9999f)
        }
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //val landsKV = enforcePartitionKV(lsOv_notnull_val,part)

      //      // Add a pause after each hour
      //      println(s"Pausing for 5 minutes after LANDSAT8 to allow UI inspection...")
      //      println(s"You can examine the Spark UI at http://localhost:4040")
      //      Thread.sleep(300000) // 5 minutes

      // Create base static layer
      val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val materize: RDD[(Int, ITile[Array[Float]])] =
        RasterOperationsLocal
          .overlayArrayStreamsF2(baseKV, landsKV) //materize.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val baseKV_full = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](RasterOperationsLocal.mapPixelsPreservePartitionerKeyedEager(materize, (temp:Array[Float]) => {temp}),part).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //baseKV_full.count()
// 51 seconds for base full eager
      // Add a pause after each hour
      //      println(s"Pausing for 5 minutes after BASE OVERLAY to allow UI inspection...")
      //      println(s"You can examine the Spark UI at http://localhost:4040")
      //      Thread.sleep(300000) // 5 minutes

      // In processDay function, update the hourly data loading section:
      // Hourly NLDAS
      val NLDAS_filter = listDateFolders(new Path("NLDAS_2023_Geotiff"), FileSystem.get(sc.hadoopConfiguration))
      val maybeFolderNLDAS: Option[String] = NLDAS_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      val NLDAS_file = maybeFolderNLDAS.getOrElse {
        throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
      }
      val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray.slice(12,18)

      var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull
      for ((path, idx) <- NLDAS_RasterRDD.zipWithIndex) {
        val hourStartTime = System.nanoTime()

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
          val hrRDD_part = hrKV_filter.retile(10,10)
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
          //val rawNext = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](BandCalculationCheck2.IterativeCalculationPre2(layered, null), part).persist(StorageLevel.MEMORY_AND_DISK_SER)
          val rawNext = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](BandCalculationCheck2.IterativeCalculationPre2(layered, null), part).persist(StorageLevel.MEMORY_AND_DISK_SER)
          if (rawNext == null) {
            println("[ERROR] BandCalculationCheck2 returned null")
          }

          // Calculate sum for data integrity check
          val sumBefore = 0
          // Delete previous checkpoint if exists
          // Calculate sum after reload
          val sumAfter = 0

          // Update cycle for next iteration
          // Unpersist the intermediate RDDs created in this iteration
          hrRDD.unpersist()
          hrKV.unpersist()
          hrKV_filter.unpersist()
          layered.unpersist()

          // Optionally unpersist previous cycle if no longer needed
          if (cycle != null) cycle.unpersist(blocking = false)
          cycle = rawNext//.persist(StorageLevel.MEMORY_AND_DISK_SER)

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

      // Print summary if we have metrics
//      if (hourlyMetrics.nonEmpty) {
//        println("\n=== Hourly Processing Summary ===")
//        println("Hour | Records | Save (MB) | Stages (Proc/Save/Load/Total) | DAG Depth | Time (ms)")
//        println("-----|---------|-----------|-------------------------------|-----------|----------")
//
//        hourlyMetrics.foreach { m =>
//          println(f"${m.hour}%4d | ${m.countAfter}%7d | ${m.saveSizeMB}%9.2f | ${m.stagesBeforeSave}%4d/${m.stagesForSave}%4d/${m.stagesForLoad}%4d/${m.totalStages}%5d | ${m.dagDepthBefore}%4d -> ${m.dagDepthAfter}%4d | ${m.processingTimeMs}%8.0f")
//        }
//      }

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
    //sc.setLogLevel("WARN")
    //conf.set("spark.cleaner.referenceTracking.blocking", "true")
    // conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    // conf.set("spark.cleaner.ttl", "3600")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val startTime = System.nanoTime()
    // Set checkpoint directory to local filesystem

    val stageCounter = new StageCountingListener
    sc.addSparkListener(stageCounter)

    // Load AOI from local filesystem
    // AOI & metadata with two-month sample period
    val aoi = sc.shapefile("CA/CA.shp")
    // sample Landsat B4 over ±1 month around the start date
    val startDate = LocalDate.parse("2021-08-10")
    val b4SampleMap = findLandsatFilesForTwoMonthWindowPaths(
      startDate,
      "California_Landsat8_4B_2021/B4",
      thresholdDays = 10,
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
    val elevRDD = sc.geoTiff[Float]("NLDAS_Elevation_2023_Geotiff").retile(10,10)
    val awcRDD = sc.geoTiff[Float]("Soil_2023/awc_gNATSGO_US.tif")
    val fcRDD = sc.geoTiff[Float]("Soil_2023/fc_gNATSGO_US.tif")
    val nlcdInt = sc.geoTiff[Int]("NLCD_2023")

    val nlcdRDD = nlcdInt.mapPixels(_.toFloat)
    val elevKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(elevRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val awcKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(awcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val fcKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(fcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val nlcdKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)

//    val elevKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeBilinear(elevRDD, _ => aoiMeta, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val awcKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeBilinear(awcRDD, _ => aoiMeta, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val fcKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeBilinear(fcRDD, _ => aoiMeta, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val nlcdKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Add a pause after each hour
    //    println(s"Pausing for 5 minutes after reshape YEARLY to allow UI inspection...")
    //    println(s"You can examine the Spark UI at http://localhost:4040")
    //    Thread.sleep(300000) // 5 minutes

    var curr = LocalDate.parse("2023-08-10")
    val end = LocalDate.parse("2023-08-10")
    var currLandsat = LocalDate.parse("2021-08-10")
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
      ) //.persist(StorageLevel.DISK_ONLY)

      // Write GeoTiff and reload every 15 days
      // Calculate days since start (1-based counting)
      // Should be:
      val processingStartDate = LocalDate.parse("2023-08-10") // Your actual start
      val daysSinceStart = java.time.temporal.ChronoUnit.DAYS.between(processingStartDate, curr).toInt + 1
      // Write GeoTiff and reload every 15 days
      if (daysSinceStart % 5 == 0) {
        println(s"[GEOTIFF CHECKPOINT] Day $daysSinceStart ($curr) - Writing intermediate results")

        // Convert to RasterRDD format for writing
        val rasterForWrite: RasterRDD[Array[Float]] = daily.values.mapPixels(arr => {
          val out = Array.fill(5)(-9999f)
          Array.copy(arr, 0, out, 0, math.min(arr.length, 5))
          out
        })
        val dateinstr = curr.toString
        val checkpointDir = "/local_data/dblab/chkpt/${dateinstr}_$idx"
        sc.setCheckpointDir(checkpointDir)
        rasterForWrite.checkpoint() // <-- Add this line to trigger checkpointing
        rasterForWrite.count() // Trigger checkpoint

        // Clean up current RDDs
        if (prevOpt.isDefined) prevOpt.get.unpersist(blocking = true)
        daily.unpersist(blocking = true)

        // Convert back to keyed format that prevOpt expects: RDD[(Int, ITile[Array[Float]])]
        val reloadedKV: RDD[(Int, ITile[Array[Float]])] = enforcePartitionKV(rasterForWrite,part) //.persist(StorageLevel.MEMORY_AND_DISK_SER)
        // Feed back into prevOpt - now it has the exact same structure as daily
        prevOpt = Some(reloadedKV)

        println(s"[LINEAGE RESET] Day $daysSinceStart - Reloaded as keyed RDD, ready for next iteration")

      } else {
        // Normal flow - daily is already in the right format
        prevOpt = Some(daily)
      }

      curr = curr.plusDays(1)
      currLandsat = currLandsat.plusDays(1)

      // Optional: JVM Heap snapshot
      val mb = 1024 * 1024
      val runtime = Runtime.getRuntime
      println(f"[Memory] Used: ${(runtime.totalMemory - runtime.freeMemory) / mb} MB, Total: ${runtime.totalMemory / mb} MB")
    }

//    val finalRdd: RasterRDD[Array[Float]] = prevOpt.get.values.mapPixels(arr => {
//      val out = Array.fill(5)(-9999f)
//      Array.copy(arr, 0, out, 0, math.min(arr.length, 5))
//      out
//    })
    val finalRdd: RasterRDD[Float] = prevOpt.get.values.mapPixels(arr => {
      arr(0)
    })
    val output = "aoidata_rdpro_landsat_ca"
    val output_file = s"$output/landsat-$star-$endDate.nn.out2.tif"
    GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(
      GeoTiffWriter.WriteMode -> "compatibility",
      GeoTiffWriter.OutputNumBands -> "1",
      GeoTiffWriter.BigTiff -> "yes",
      GeoTiffWriter.FillValue -> "-9999"
    ))
    val endTime = System.nanoTime()
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
    //StdIn.readLine()
    spark.stop()
  }
}
