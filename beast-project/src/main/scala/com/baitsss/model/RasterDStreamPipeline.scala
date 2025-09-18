//package com.baitsss.model
//import java.io.File
//import java.nio.file.{Files, Paths, StandardOpenOption}
//import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
//import java.time.format.DateTimeFormatter
//import java.util.concurrent.atomic.AtomicBoolean
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.io.{LongWritable, Text}
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
//import org.apache.log4j.Logger
//import org.apache.spark._
//import org.apache.spark.HashPartitioner
//import org.apache.spark.rdd.RDD
//import org.apache.spark.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted}
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming._
//import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
//import edu.ucr.cs.bdlab.raptor.{RasterOperationsFocal, RasterOperationsLocal}
//import com.baitsss.model.PipelineLandsatETModel
//import com.baitsss.model.PipelineLandsatETModel2.extractMonthDay
//import com.baitsss.model.PipelineLandsatETModel3.findLocalLandsatFiles
//import edu.ucr.cs.bdlab.beast.{RasterReadMixinFunctions, ReadWriteMixinFunctions}
//import org.geotools.referencing.CRS
//
//import scala.collection.mutable.ArrayBuffer
//
//object RasterDStreamPipeline {
//  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
//
//  // ─────────── Shared mutable state ────────────────────────────────────────────
//  @volatile private var prevHourRDD: RDD[(Int, ITile[Array[Float]])] = null
//  @volatile private var currentDay: LocalDate                           = null
//  private val isProcessing  = new AtomicBoolean(false)
//  private val pending       = scala.collection.mutable.Queue[String]()
//  private val zone          = ZoneId.systemDefault()
//  private val dateFmt       = DateTimeFormatter.ofPattern("yyyy-MM-dd")
//
//  // ─────────── Paths & directories ────────────────────────────────────────────
//  val basePath            = "/Users/clockorangezoe/Desktop/BAITSSS_project"
//  val triggerDir          = s"$basePath/triggers"
//  val dailyStateDir       = s"$basePath/daily-state"
//  val hourlyStateDir      = s"$basePath/hourly-state"
//  val completionMarkerDir = s"$basePath/completed"
//
//  def main(args: Array[String]): Unit = {
//    // ─── Spark & StreamingContext setup ────────────────────────────────────────
//    val conf = new SparkConf()
//      .setAppName("RasterDStreamPipeline")
//      .setIfMissing("spark.master", "local[*]")
//      .set("spark.streaming.concurrentJobs", "1") // ensure no overlapping batches
//
//    val sc  = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val ssc = new StreamingContext(sc, Seconds(60))
//    ssc.checkpoint(s"$basePath/checkpoints")
//
//    // Add a listener to log batch start/finish times
//    ssc.addStreamingListener(new StreamingListener {
//      override def onBatchStarted(ev: StreamingListenerBatchStarted): Unit =
//        log.info(s"[Listener] Batch ${ev.batchInfo.batchTime} started at ${Instant.now()}")
//
//      override def onBatchCompleted(ev: StreamingListenerBatchCompleted): Unit = {
//        val delay = ev.batchInfo.processingDelay.getOrElse(-1L)
//        log.info(s"[Listener] Batch ${ev.batchInfo.batchTime} completed at ${Instant.now()}, delay=${delay}ms")
//      }
//    })
//
//    // Ensure state directories exist
//    Seq(dailyStateDir, hourlyStateDir, completionMarkerDir).foreach(dir => new File(dir).mkdirs())
//
//    // ─── 1) Load AOI & compute metadata ──────────────────────────────────────────
//    val aoiPath = s"$basePath/aoi/POLYGON.shp"
//    val aoi     = sc.shapefile(aoiPath)
//    val sample: RDD[ITile[Int]]  = sc.geoTiff[Int](
//      PipelineLandsatETModel.findLandsatFilesForTwoMonthWindowPaths(
//        LocalDate.parse("2021-01-01"),
//        "file:///Users/clockorangezoe/Desktop/BAITSSS_project/data/California_Landsat8_4B_2021/B4",
//        thresholdDays = 10,
//        sc.hadoopConfiguration
//      ).values.flatten.mkString(" ")
//    )
//    val initial = sample.first().rasterMetadata
//    val globalMeta = PipelineLandsatETModel2.Raster_metadata(initial, RasterMetadata.allMetadata(sample), CRS.decode("EPSG:4326"))
//    val aoiMeta = PipelineLandsatETModel2.AOI_metadata(aoi, globalMeta)
//    // Partitioning setup
//    val parts = sample.getNumPartitions
//    val part  = new HashPartitioner(parts)
//
//    // ─── 2) Build static annual RDD once ─────────────────────────────────────────
//    log.info("Building static annual layers (soil, elev, NLCD)…")
//    // Load elevation, soil, and NLCD data from local filesystem
//    val elevRDD = sc.geoTiff[Float]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/NLDAS_elevation.tif") // Update
//    val awcRDD = sc.geoTiff[Float]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/inputdata/soil_awc") // Update
//    val fcRDD = sc.geoTiff[Float]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/inputdata/soil_fc") // Update
//    val nlcdInt = sc.geoTiff[Int]("file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/nlcd32_4326_LZW.tif") // Update
//
//    val nlcdRDD = nlcdInt.mapPixels(_.toFloat)
//    val elevKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(elevRDD, _ => aoiMeta, parts, part).persist(StorageLevel.DISK_ONLY)
//    val soilAWCKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(awcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.DISK_ONLY)
//    val soilFCKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(fcRDD, _ => aoiMeta, parts, part).persist(StorageLevel.DISK_ONLY)
//    val nlcdKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(nlcdRDD, _ => aoiMeta, parts, part).persist(StorageLevel.DISK_ONLY)
//    val prism_daily = "file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/inputdata/prism/PRISM_tmin_stable_4kmD2_20190923_bil_4326.tif"
//    val targetMonthDay = extractMonthDay(        LocalDate.parse("2023-01-01"))
//    println(s"[DEBUG] Looking for month-day: $targetMonthDay")
//    println(s"[DEBUG] Loading PRISM from: $prism_daily")
//    val prismKV: RDD[(Int, ITile[Float])] = RasterOperationsFocal.reshapeNN2(
//        sc.geoTiff[Float](prism_daily), _ => aoiMeta, parts, part)
//      .persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val b4Map = findLocalLandsatFiles(
//      LocalDate.parse("2021-01-01"),
//      "/Users/clockorangezoe/Desktop/BAITSSS_project/data/California_Landsat8_4B_2021/B4",
//      10
//    )
//    val b5Map = findLocalLandsatFiles(
//      LocalDate.parse("2021-01-01"),
//      "/Users/clockorangezoe/Desktop/BAITSSS_project/data/California_Landsat8_4B_2021/B5",
//      10
//    )
//
//    val b4RDD = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
//    val b5RDD = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
//    val rs4: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b4RDD, _ => aoiMeta, parts, part)
//    val rs5: RDD[(Int, ITile[Int])] = RasterOperationsFocal.reshapeNN2(b5RDD, _ => aoiMeta, parts, part)
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
//    })
//    val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
//      .persist(StorageLevel.DISK_ONLY)
//
//    // ─── 3) Prepare trigger DStream for hourly triggers ─────────────────────────
//    val triggerStream: DStream[String] =
//      ssc.fileStream[LongWritable, Text, TextInputFormat](
//        triggerDir,
//        // only accept files ending in .trigger
//        (path: Path) => path.getName.endsWith(".trigger"),
//        newFilesOnly = true
//      ).map { case (_, _, path) => path.toString }
//
//    // Enqueue new triggers and immediately attempt to drain the queue
//    triggerStream.foreachRDD { (rdd, _) =>
//      val it = rdd.toLocalIterator
//      synchronized {
//        it.foreach(pending.enqueue(_))
//        if (pending.nonEmpty)
//          log.info(s"Enqueued triggers; queue size=${pending.size}")
//      }
//      processPending(sc, annualStatic, aoiMeta, parts, part)
//    }
//
//    // ─── 4) Start streaming ─────────────────────────────────────────────────────
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  /** Drain the pending queue, processing one trigger (hour) at a time. */
//  def processPending(
//                      sc: SparkContext,
//                      annualStatic: RDD[(Int, ITile[Float])],
//                      aoiMeta: RasterMetadata,
//                      parts: Int,
//                      part: HashPartitioner
//                    ): Unit = synchronized {
//    while (!isProcessing.get() && pending.nonEmpty) {
//      val triggerPath = pending.dequeue()
//      isProcessing.set(true)
//      try {
//        log.info(s"Processing trigger: $triggerPath")
//        val dayDir = new File(s"/Users/clockorangezoe/Desktop/BAITSSS_project/data/aoidata_nldas_all_reshape/$dateStr")
//        println(s"[DEBUG] Checking NLDAS path: ${dayDir.getAbsolutePath}")
//        println(s"[DEBUG] NLDAS exists: ${dayDir.exists()}, isDir: ${dayDir.isDirectory}")
//        // Replace your current tifPaths code with this:
//        val tifPaths = if (dayDir.exists() && dayDir.isDirectory) {
//          // Look for TIF files in all sub-directories
//          def findAllTifFiles(dir: File): Array[String] = {
//            val result = new ArrayBuffer[String]()
//            if (dir.exists() && dir.isDirectory) {
//              dir.listFiles().foreach { file =>
//                if (file.isFile && file.getName.toLowerCase.endsWith(".tif")) {
//                  result += s"file://${file.getAbsolutePath}"
//                } else if (file.isDirectory) {
//                  result ++= findAllTifFiles(file)
//                }
//              }
//            }
//            result.toArray
//          }
//
//          val allTifFiles = findAllTifFiles(dayDir).sorted
//          println(s"[DEBUG] Found ${allTifFiles.length} TIF files")
//          allTifFiles.foreach(path => println(s"[DEBUG] TIF: $path"))
//          allTifFiles
//        } else {
//          println(s"[DEBUG] No NLDAS directory found")
//          Array.empty[String]
//        }
//
//        if (tifPaths.isEmpty) {
//          println(s"[WARN] No hourly TIF files found for $dateStr")
//          return prevOpt.orNull
//        }
//
//        println(s"[DEBUG] Processing ${tifPaths.length} hourly files")
//
//        var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull
//        println(s"[DEBUG] Initial cycle is null: ${cycle == null}")
//
//
//        val (date, hour) = PipelineLandsatETModel.parseTrigger(triggerPath)
//
//        // ─── If first trigger of a new day, build & reload daily base ─────────────
//        if (currentDay == null || date.isAfter(currentDay)) {
//          currentDay = date
//          log.info(s"Building daily base for $date")
//
//          val dailyBase: RDD[(Int, ITile[Array[Float]])] =
//            PipelineLandsatETModel.buildDailyBase(sc, annualStatic, aoiMeta, parts, part, date)
//
//          // Truncate its lineage by saving & reloading
//          val dailyPath = s"$dailyStateDir/day-$date"
//          dailyBase.saveAsObjectFile(dailyPath)
//          dailyBase.unpersist(true)
//
//          val reloadedDaily = sc.objectFile[(Int, ITile[Array[Float]])](dailyPath)
//            .partitionBy(part)
//            .persist(StorageLevel.DISK_ONLY)
//          reloadedDaily.count()
//
//          // Initialize prevHourRDD for the first hour
//          if (prevHourRDD != null) prevHourRDD.unpersist(true)
//          prevHourRDD = reloadedDaily
//        }
//
//        // ─── Process this single hour ─────────────────────────────────────────────
//        val dailyBase = prevHourRDD  // holds the daily base for hour 1, or last state afterwards
//
//        val nldasPath = PipelineLandsatETModel.constructNLDASPath(date, hour)
//        log.info(s"Loading hourly NLDAS: $nldasPath")
//        val hourlyTileRDD = sc.geoTiff[Array[Float]](nldasPath)
//        val hourlyKV = RasterOperationsFocal.reshapeNN2(hourlyTileRDD, _ => aoiMeta, parts, part)
//
//        // Truncate incoming lineage
//        hourlyKV.persist(StorageLevel.DISK_ONLY)
//        hourlyKV.checkpoint(); hourlyKV.count(); hourlyKV.unpersist(true)
//
//        // Overlay daily + hourly + previous hour’s state
//        val overlay1 = RasterOperationsLocal.overlayArrayStreamsF2(dailyBase, hourlyKV)
//        val overlay2 = RasterOperationsLocal.overlayArrayStreamsF2(overlay1, prevHourRDD)
//
//        // Iterative calculation step
//        val nextState = BandCalculationCheck2.IterativeCalculationPre2(overlay2, null)
//
//        // Save & reload to truncate lineage for next iteration
//        val outPath = s"$hourlyStateDir/day-$date-hour-$hour"
//        nextState.saveAsObjectFile(outPath); nextState.unpersist(true)
//        val reloadedNext = sc.objectFile[(Int, ITile[Array[Float]])](outPath)
//          .partitionBy(part)
//          .persist(StorageLevel.DISK_ONLY)
//        reloadedNext.count()
//
//        // Cleanup & swap
//        hourlyKV.unpersist(true)
//        if (prevHourRDD != null) prevHourRDD.unpersist(true)
//        prevHourRDD = reloadedNext
//
//        // Mark completion
//        val marker = Paths.get(s"$completionMarkerDir/${date}_${hour}.complete")
//        Files.write(marker, LocalDateTime.now().toString.getBytes(), StandardOpenOption.CREATE_NEW)
//
//        log.info(s"✅ Completed $date hour $hour")
//
//      } catch {
//        case e: Throwable =>
//          log.error(s"Error processing trigger $triggerPath", e)
//      } finally {
//        isProcessing.set(false)
//      }
//    }
//  }
//}