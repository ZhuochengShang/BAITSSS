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
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.awt.geom.Point2D
import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag
import scala.collection.mutable

object PipelineLandsatETModelNP_hour {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val mdFmt = DateTimeFormatter.ofPattern("MM-dd")
  private val checkpointInterval = 2
  private val starttime = System.nanoTime()

  // Enhanced metrics tracking
  case class DetailedMetrics(
                              operation: String,
                              startTime: Long,
                              endTime: Long,
                              stagesBefore: Int,
                              stagesAfter: Int,
                              shuffleReadBytes: Long,
                              shuffleWriteBytes: Long,
                              shuffleReadRecords: Long,
                              shuffleWriteRecords: Long,
                              dagDepth: Int,
                              lineageString: String,
                              memoryUsedMB: Double,
                              recordCount: Long,
                              partitionCount: Int
                            ) {
    def durationMs: Double = (endTime - startTime) / 1e6
    def shuffleReadMB: Double = shuffleReadBytes / (1024.0 * 1024.0)
    def shuffleWriteMB: Double = shuffleWriteBytes / (1024.0 * 1024.0)
    def totalShuffleMB: Double = shuffleReadMB + shuffleWriteMB
  }

  // Enhanced listener for comprehensive tracking
  class ComprehensiveSparkListener extends SparkListener {
    private val stageMetrics = mutable.Map[Int, StageInfo]()
    private var totalShuffleReadBytes = 0L
    private var totalShuffleWriteBytes = 0L
    private var totalShuffleReadRecords = 0L
    private var totalShuffleWriteRecords = 0L
    private var totalStages = 0
    private var currentOperationStages = 0

    case class StageInfo(
                          stageId: Int,
                          stageName: String,
                          submissionTime: Long,
                          var completionTime: Long = 0L,
                          var shuffleReadBytes: Long = 0L,
                          var shuffleWriteBytes: Long = 0L,
                          var shuffleReadRecords: Long = 0L,
                          var shuffleWriteRecords: Long = 0L
                        )

    def reset(): Unit = {
      currentOperationStages = 0
      stageMetrics.clear()
    }

    def getAndResetMetrics(): (Int, Long, Long, Long, Long) = {
      val stages = currentOperationStages
      val readBytes = totalShuffleReadBytes
      val writeBytes = totalShuffleWriteBytes
      val readRecords = totalShuffleReadRecords
      val writeRecords = totalShuffleWriteRecords

      reset()
      totalShuffleReadBytes = 0L
      totalShuffleWriteBytes = 0L
      totalShuffleReadRecords = 0L
      totalShuffleWriteRecords = 0L

      (stages, readBytes, writeBytes, readRecords, writeRecords)
    }

    override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
      val stageInfo = StageInfo(
        event.stageInfo.stageId,
        event.stageInfo.name,
        event.stageInfo.submissionTime.getOrElse(System.currentTimeMillis())
      )
      stageMetrics(event.stageInfo.stageId) = stageInfo
      currentOperationStages += 1
      totalStages += 1
    }

    override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
      val stageId = event.stageInfo.stageId
      stageMetrics.get(stageId) match {
        case Some(info) =>
          info.completionTime = event.stageInfo.completionTime.getOrElse(System.currentTimeMillis())
          val metrics = event.stageInfo.taskMetrics

          info.shuffleReadBytes = metrics.shuffleReadMetrics.totalBytesRead
          info.shuffleWriteBytes = metrics.shuffleWriteMetrics.bytesWritten
          info.shuffleReadRecords = metrics.shuffleReadMetrics.recordsRead
          info.shuffleWriteRecords = metrics.shuffleWriteMetrics.recordsWritten

          totalShuffleReadBytes += info.shuffleReadBytes
          totalShuffleWriteBytes += info.shuffleWriteBytes
          totalShuffleReadRecords += info.shuffleReadRecords
          totalShuffleWriteRecords += info.shuffleWriteRecords

        case None =>
          println(s"[WARNING] Stage $stageId completed but not found in tracking")
      }
    }

    def printStageDetails(): Unit = {
      println("\n=== Detailed Stage Metrics ===")
      println("Stage ID | Stage Name | Duration (ms) | Shuffle Read (MB) | Shuffle Write (MB) | Read Records | Write Records")
      println("---------|------------|---------------|-------------------|-------------------|--------------|---------------")

      stageMetrics.values.toSeq.sortBy(_.stageId).foreach { info =>
        val duration = if (info.completionTime > 0) info.completionTime - info.submissionTime else 0L
        val readMB = info.shuffleReadBytes / (1024.0 * 1024.0)
        val writeMB = info.shuffleWriteBytes / (1024.0 * 1024.0)

        println(f"${info.stageId}%8d | ${info.stageName.take(10)}%10s | ${duration}%13d | ${readMB}%17.2f | ${writeMB}%18.2f | ${info.shuffleReadRecords}%12d | ${info.shuffleWriteRecords}%14d")
      }
    }
  }

  // Utility functions for tracking
  def estimateDAGDepth(rdd: RDD[_]): Int = {
    val indentLengths = rdd.toDebugString.linesIterator.map(_.takeWhile(_ == ' ').length).toList
    if (indentLengths.nonEmpty) (indentLengths.max / 2) + 1 else 1
  }

  def getLineageString(rdd: RDD[_], maxLines: Int = 10): String = {
    rdd.toDebugString.linesIterator.take(maxLines).mkString("|")
  }

  def getMemoryUsageMB(): Double = {
    val runtime = Runtime.getRuntime
    (runtime.totalMemory - runtime.freeMemory) / (1024.0 * 1024.0)
  }

  def trackOperation[T](
                         operationName: String,
                         listener: ComprehensiveSparkListener
                       )(operation: => T): (T, DetailedMetrics) = {

    val startTime = System.nanoTime()
    listener.reset()

    println(s"\n[TRACKING] Starting operation: $operationName")

    val result = operation

    val endTime = System.nanoTime()
    val (stages, shuffleReadBytes, shuffleWriteBytes, shuffleReadRecords, shuffleWriteRecords) =
      listener.getAndResetMetrics()

    val metrics = result match {
      case rdd: RDD[_] =>
        DetailedMetrics(
          operation = operationName,
          startTime = startTime,
          endTime = endTime,
          stagesBefore = 0, // Could be enhanced to track before/after
          stagesAfter = stages,
          shuffleReadBytes = shuffleReadBytes,
          shuffleWriteBytes = shuffleWriteBytes,
          shuffleReadRecords = shuffleReadRecords,
          shuffleWriteRecords = shuffleWriteRecords,
          dagDepth = estimateDAGDepth(rdd),
          lineageString = getLineageString(rdd),
          memoryUsedMB = getMemoryUsageMB(),
          recordCount = 0L, // Would need count() call to populate
          partitionCount = rdd.getNumPartitions
        )
      case _ =>
        DetailedMetrics(
          operation = operationName,
          startTime = startTime,
          endTime = endTime,
          stagesBefore = 0,
          stagesAfter = stages,
          shuffleReadBytes = shuffleReadBytes,
          shuffleWriteBytes = shuffleWriteBytes,
          shuffleReadRecords = shuffleReadRecords,
          shuffleWriteRecords = shuffleWriteRecords,
          dagDepth = 0,
          lineageString = "",
          memoryUsedMB = getMemoryUsageMB(),
          recordCount = 0L,
          partitionCount = 0
        )
    }

    println(f"[TRACKING] Completed $operationName in ${metrics.durationMs}%.2f ms")
    println(f"           Stages: ${metrics.stagesAfter}, DAG Depth: ${metrics.dagDepth}")
    println(f"           Shuffle: Read ${metrics.shuffleReadMB}%.2f MB, Write ${metrics.shuffleWriteMB}%.2f MB")
    println(f"           Memory: ${metrics.memoryUsedMB}%.2f MB, Partitions: ${metrics.partitionCount}")

    (result, metrics)
  }

  // Rest of your existing functions remain the same...
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

    val targetMetadataTmp = RasterMetadata.create(minX1, maxY2, maxX2, minY1, 4326, rasterWidth, rasterHeight, 10, 10)
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

  def extractMonthDay(date: LocalDate): String = date.format(mdFmt)

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
                  listener: ComprehensiveSparkListener
                ): RDD[(Int, ITile[Array[Float]])] = {

    val dateStr = datein.toString
    println(s"\n[DEBUG] === Processing day $dateStr ===")

    val dayMetrics = mutable.ArrayBuffer[DetailedMetrics]()

    try {
      val startDaytime = System.nanoTime()
      val targetMonthDay = extractMonthDay(datein)
      val prism_daily = "PRISM_2023_Geotiff"
      val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration))
      val maybeFolderPrism: Option[String] = PRISM_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      // Track PRISM loading
      val (prismKV, prismMetrics) = trackOperation("PRISM_Loading", listener) {
        RasterOperationsFocal.reshapeNN2(
          sc.geoTiff[Float](maybeFolderPrism.get), _ => metadata, numParts, part
        ).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      dayMetrics += prismMetrics

      // Track Landsat processing
      val (landsKV, landsatMetrics): (RDD[(Int, ITile[Array[Float]])], DetailedMetrics)  = trackOperation("Landsat_Processing", listener) {
        val b4Map = findLandsatFilesForTwoMonthWindowPaths(landsatDate, "California_Landsat8_4B_2021/B4", 10, sc.hadoopConfiguration)
        val b5Map = findLandsatFilesForTwoMonthWindowPaths(landsatDate, "California_Landsat8_4B_2021/B5", 10, sc.hadoopConfiguration)
        val b4RDD: RasterRDD[Int] = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
        val b5RDD: RasterRDD[Int] = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
        val rs4: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b4RDD, _ => metadata, numParts, part)
        val rs5: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b5RDD, _ => metadata, numParts, part)
        val lsOv: RDD[(Int, ITile[Array[Int]])] = RasterOperationsLocal.overlayArrayStreamsF(rs4, rs5)
        val result: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(lsOv, (p: Array[Int]) => {
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
        }).persist(StorageLevel.MEMORY_AND_DISK_SER)
        result
      }
      dayMetrics += landsatMetrics

      // Track base overlay creation
//      val (baseKV_full, baseMetrics) = trackOperation("Base_Overlay", listener) {
//        val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
//        RasterOperationsLocal
//          .overlayArrayStreamsF2(baseKV, landsKV).persist(StorageLevel.MEMORY_AND_DISK_SER)
//      }
//      dayMetrics += baseMetrics

      // Track base overlay creation using overlay method for comparison
      val (baseKV, baseMetrics1): (RDD[(Int,ITile[Array[Float]])], DetailedMetrics) = trackOperation("Base_Overlay_Step1", listener) {
        val result:RDD[(Int,ITile[Array[Float]])] = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
        val count = result.count() // Force materialization
        println(s"[MATERIALIZED] Base_Overlay_Step1: $count records")
        result
      }

      val (baseKV_full, baseMetrics2): (RDD[(Int, ITile[Array[Float]])], DetailedMetrics) = trackOperation("Base_Overlay_Step2", listener) {
        val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val materize: RDD[(Int, ITile[Array[Float]])] =
          RasterOperationsLocal
            .overlayArrayStreamsF2(baseKV, landsKV) //materize.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val result = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](RasterOperationsLocal.mapPixelsPreservePartitionerKeyedEager(materize, (temp: Array[Float]) => {
          temp
        }), part).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val count = result.count() // Force materialization
        println(s"[MATERIALIZED] Base_Overlay_Step2: $count records")
        result
      }
      dayMetrics += baseMetrics1
      dayMetrics += baseMetrics2

      // Hourly NLDAS processing with tracking
      val NLDAS_filter = listDateFolders(new Path("NLDAS_2023_Geotiff"), FileSystem.get(sc.hadoopConfiguration))
      val maybeFolderNLDAS: Option[String] = NLDAS_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      val NLDAS_file = maybeFolderNLDAS.getOrElse {
        throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
      }
      val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray//.slice(0,2)

      var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull

      for ((path, idx) <- NLDAS_RasterRDD.zipWithIndex) {
        println(s"\n=== Hour ${idx + 1} Processing ===")

        try {
          val (newCycle, hourMetrics) = trackOperation(s"Hour_${idx + 1}_Processing", listener) {
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

            // Overlay operation
            val (layered, overlayMetrics): (RDD[(Int, ITile[Array[Float]])], DetailedMetrics) = trackOperation(s"Hour_${idx + 1}_Overlay", listener) {
              if (cycle == null) {
                println("[DEBUG] First hour - no previous cycle")
                val result:RDD[(Int,ITile[Array[Float]])] = RasterOperationsLocal.overlayArrayStreamsF2(baseKV_full, hrKV)
                val count = result.count() // Force materialization
                println(s"[MATERIALIZED] Base_Overlay_Step2: $count records")
                result
              } else {
                println("[DEBUG] Continuing from previous cycle")
                val result:RDD[(Int,ITile[Array[Float]])] = RasterOperationsLocal.overlayArrayStreamsF2(baseKV_full, hrKV, cycle)
                val count = result.count() // Force materialization
                println(s"[MATERIALIZED] Base_Overlay_Step2: $count records")
                result
              }
            }
            dayMetrics += overlayMetrics

            // Calculate next iteration
//            val rawNext = new ShuffledRDD[Int, ITile[Array[Float]], ITile[Array[Float]]](
//              BandCalculationCheck2.IterativeCalculationPre2(layered, null), part
//            ).persist(StorageLevel.MEMORY_AND_DISK_SER)
            val rawNext =
              BandCalculationCheck2.IterativeCalculationSimple(layered, null).persist(StorageLevel.MEMORY_AND_DISK_SER)
//            val rawNext =
//              BandCalculationCheck2.IterativeCalculationPre2(layered, null).persist(StorageLevel.MEMORY_AND_DISK_SER)





            // Cleanup intermediate RDDs
            hrRDD.unpersist()
            hrKV.unpersist()
            hrKV_filter.unpersist()
            layered.unpersist()

            if (cycle != null) cycle.unpersist(blocking = false)

            rawNext
          }

          dayMetrics += hourMetrics
          cycle = newCycle

          println(f"✅ Hour ${idx + 1} completed in ${hourMetrics.durationMs}%.2f ms")
          println(f"   Shuffle: ${hourMetrics.totalShuffleMB}%.2f MB total")

        } catch {
          case e: Exception =>
            println(s"[ERROR] Failed processing hour ${idx + 1}: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      // Print comprehensive day summary
      println(s"\n=== Day $dateStr Processing Summary ===")
      println("Operation | Duration (ms) | Stages | Shuffle Read (MB) | Shuffle Write (MB) | DAG Depth | Memory (MB)")
      println("----------|---------------|--------|-------------------|--------------------|-----------|-----------")

      dayMetrics.foreach { m =>
        println(f"${m.operation.take(15)}%-15s | ${m.durationMs}%13.0f | ${m.stagesAfter}%6d | ${m.shuffleReadMB}%17.2f | ${m.shuffleWriteMB}%18.2f | ${m.dagDepth}%9d | ${m.memoryUsedMB}%9.1f")
      }

      val totalShuffleMB = dayMetrics.map(_.totalShuffleMB).sum
      val totalStages = dayMetrics.map(_.stagesAfter).sum
      val totalDuration = dayMetrics.map(_.durationMs).sum

      println(f"\nDay Totals: Duration: ${totalDuration}%.0f ms, Stages: $totalStages, Shuffle: ${totalShuffleMB}%.2f MB")

      // Cleanup
      prismKV.unpersist()
      baseKV_full.unpersist()

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
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val startTime = System.nanoTime()

    // Enhanced listener setup
    val comprehensiveListener = new ComprehensiveSparkListener
    sc.addSparkListener(comprehensiveListener)

    println("=== Enhanced Monitoring Enabled ===")
    println("Tracking: Shuffle volume, stages, lineage depth, memory usage")

    // Load AOI and setup metadata (unchanged)
    val aoi = sc.shapefile("CA/CA.shp")
    val startDate = LocalDate.parse("2021-01-01")
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

    // Load static layers with tracking and materialization
    val (elevKV, _) = trackOperation("Load_Elevation", comprehensiveListener) {
      val elevRDD = sc.geoTiff[Float]("NLDAS_Elevation_2023_Geotiff")
      val result = RasterOperationsFocal.reshapeNN2(elevRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val count = result.count() // Force materialization
      println(s"[MATERIALIZED] Elevation: $count records")
      result
    }

    val (awcKV, _) = trackOperation("Load_AWC", comprehensiveListener) {
      val awcRDD = sc.geoTiff[Float]("Soil_2023/awc_gNATSGO_US.tif")
      val result = RasterOperationsFocal.reshapeNN2(awcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val count = result.count() // Force materialization
      println(s"[MATERIALIZED] AWC: $count records")
      result
    }

    val (fcKV, _) = trackOperation("Load_FC", comprehensiveListener) {
      val fcRDD = sc.geoTiff[Float]("Soil_2023/fc_gNATSGO_US.tif")
      val result = RasterOperationsFocal.reshapeNN2(fcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val count = result.count() // Force materialization
      println(s"[MATERIALIZED] FC: $count records")
      result
    }

    val (nlcdKV, _) = trackOperation("Load_NLCD", comprehensiveListener) {
      val nlcdInt = sc.geoTiff[Int]("NLCD_2023")
      val nlcdRDD = nlcdInt.mapPixels(_.toFloat)
      val result = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => aoiMeta, parts, part).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val count = result.count() // Force materialization
      println(s"[MATERIALIZED] NLCD: $count records")
      result
    }

    // Daily processing loop with comprehensive tracking
    var curr = LocalDate.parse("2023-01-01")
    val end = LocalDate.parse("2023-01-01")
    var currLandsat = LocalDate.parse("2021-01-01")
    val endDate = end.toString
    var star = curr.toString
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]] = None

    val dailyMetrics = mutable.ArrayBuffer[DetailedMetrics]()
    var cumulativeShuffleMB = 0.0
    var cumulativeStages = 0

    while (!curr.isAfter(end)) {
      println(s"\n=== [Day ${curr}] Processing ===")

      val (daily, dayMetrics) = trackOperation(s"Day_${curr}_Complete", comprehensiveListener) {
        processDay(
          spark, sc, curr, currLandsat, aoi, aoiMeta, parts, part,
          prevOpt, awcKV, fcKV, nlcdKV, elevKV, comprehensiveListener
        )
      }

      dailyMetrics += dayMetrics
      cumulativeShuffleMB += dayMetrics.totalShuffleMB
      cumulativeStages += dayMetrics.stagesAfter

      println(s"[Day Summary] $curr: ${dayMetrics.durationMs / 1000.0}%.1f sec, ${dayMetrics.stagesAfter} stages, ${dayMetrics.totalShuffleMB}%.1f MB shuffle")
      println(s"[Cumulative] Total: ${cumulativeStages} stages, ${cumulativeShuffleMB}%.1f MB shuffle")
      println(s"[Lineage Depth] ${dayMetrics.dagDepth} levels")

      // Print detailed stage information periodically
      if (curr.getDayOfMonth % 2 == 0) {
        comprehensiveListener.printStageDetails()
      }

      prevOpt = Some(daily)
      curr = curr.plusDays(1)
      currLandsat = currLandsat.plusDays(1)

      // Memory monitoring
      val mb = 1024 * 1024
      val runtime = Runtime.getRuntime
      println(f"[Memory] Used: ${(runtime.totalMemory - runtime.freeMemory) / mb} MB, Total: ${runtime.totalMemory / mb} MB")

      // Optional garbage collection hint for long-running jobs
      if (curr.getDayOfMonth % 3 == 0) {
        System.gc()
        println("[GC] Garbage collection hint issued")
      }
    }

    // Final processing and output with tracking
    val (finalOutput, outputMetrics) = trackOperation("Final_Output_Generation", comprehensiveListener) {
      val finalRdd: RasterRDD[Array[Float]] = prevOpt.get.values.mapPixels(arr => {
        val out = Array.fill(5)(-9999f)
        Array.copy(arr, 0, out, 0, math.min(arr.length, 5))
        out
      })

      val output = "aoidata_rdpro_landsat_ca"
      val output_file = s"$output/landsat-$star-$endDate.nn.tif"
      GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(
        GeoTiffWriter.WriteMode -> "compatibility",
        GeoTiffWriter.OutputNumBands -> "5",
        GeoTiffWriter.BigTiff -> "yes",
        GeoTiffWriter.FillValue -> "-9999"
      ))
      finalRdd
    }

    dailyMetrics += outputMetrics

    val endTime = System.nanoTime()
    val totalTimeSeconds = (endTime - startTime) / 1E9

    // Comprehensive final report
    println("\n" + "=" * 80)
    println("COMPREHENSIVE PROCESSING REPORT")
    println("=" * 80)

    println(f"\n📊 OVERALL METRICS:")
    println(f"   Total Time: ${totalTimeSeconds}%.1f seconds (${totalTimeSeconds / 60.0}%.1f minutes)")
    println(f"   Total Stages: $cumulativeStages")
    println(f"   Total Shuffle: ${cumulativeShuffleMB}%.1f MB (${cumulativeShuffleMB / 1024.0}%.2f GB)")
    println(f"   Average Shuffle per Day: ${cumulativeShuffleMB / dailyMetrics.length}%.1f MB")

    println(f"\n📈 DAILY BREAKDOWN:")
    println("Day | Duration (min) | Stages | Shuffle (MB) | DAG Depth | Memory (MB)")
    println("----|----------------|--------|--------------|-----------|-----------")

    dailyMetrics.foreach { m =>
      val dayStr = m.operation.replace("Day_", "").replace("_Complete", "")
      println(f"$dayStr%-3s | ${m.durationMs / 60000.0}%14.2f | ${m.stagesAfter}%6d | ${m.totalShuffleMB}%12.1f | ${m.dagDepth}%9d | ${m.memoryUsedMB}%9.1f")
    }

    println(f"\n🔍 PERFORMANCE INSIGHTS:")
    val avgStagesPerDay = cumulativeStages.toDouble / dailyMetrics.length
    val avgShufflePerDay = cumulativeShuffleMB / dailyMetrics.length
    val maxDagDepth = dailyMetrics.map(_.dagDepth).max
    val avgDagDepth = dailyMetrics.map(_.dagDepth).sum.toDouble / dailyMetrics.length

    println(f"   Average stages per day: ${avgStagesPerDay}%.1f")
    println(f"   Average shuffle per day: ${avgShufflePerDay}%.1f MB")
    println(f"   Maximum DAG depth: $maxDagDepth")
    println(f"   Average DAG depth: ${avgDagDepth}%.1f")

    // Identify potential optimization opportunities
    println(f"\n💡 OPTIMIZATION OPPORTUNITIES:")
    val highShuffleDays = dailyMetrics.filter(_.totalShuffleMB > avgShufflePerDay * 1.5)
    val highStageDays = dailyMetrics.filter(_.stagesAfter > avgStagesPerDay * 1.5)
    val deepLineageDays = dailyMetrics.filter(_.dagDepth > avgDagDepth * 1.5)

    if (highShuffleDays.nonEmpty) {
      println(f"   High shuffle days (>${avgShufflePerDay * 1.5}%.1f MB): ${highShuffleDays.map(_.operation).mkString(", ")}")
    }
    if (highStageDays.nonEmpty) {
      println(f"   High stage count days (>${avgStagesPerDay * 1.5}%.0f stages): ${highStageDays.map(_.operation).mkString(", ")}")
    }
    if (deepLineageDays.nonEmpty) {
      println(f"   Deep lineage days (>${avgDagDepth * 1.5}%.1f levels): ${deepLineageDays.map(_.operation).mkString(", ")}")
    }

    if (maxDagDepth > 10) {
      println("   ⚠️  Consider more frequent checkpointing to reduce lineage depth")
    }
    if (avgShufflePerDay > 1000) {
      println("   ⚠️  High shuffle volume detected - consider repartitioning strategies")
    }
    if (avgStagesPerDay > 50) {
      println("   ⚠️  High stage count - consider operation fusion opportunities")
    }

    println(f"\n🎯 RECOMMENDATIONS:")
    println("   1. Monitor shuffle-heavy operations for repartitioning opportunities")
    println("   2. Consider checkpointing strategies for operations with deep lineage")
    println("   3. Profile memory usage patterns for optimal caching decisions")
    println("   4. Evaluate partition count vs. data size ratios")

    println("\n" + "=" * 80)

    // Final stage details
    comprehensiveListener.printStageDetails()

    spark.stop()
  }
}
