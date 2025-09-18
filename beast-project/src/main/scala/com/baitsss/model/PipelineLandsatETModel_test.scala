package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.enforcePartitionKV
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
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
import scala.collection.mutable.ListBuffer

object PipelineLandsatETModel_test {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val mdFmt = DateTimeFormatter.ofPattern("MM-dd")
  private val checkpointInterval = 6 //4 //6 // checkpoint every 4 hours to truncate lineage
  private val starttime = System.nanoTime()

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

  def AOI_metadata(AOI: RDD[IFeature], targetMetadata: RasterMetadata): RasterMetadata = {
    val corner1: Point2D.Double = new Point2D.Double
    val corner2: Point2D.Double = new Point2D.Double

    val geom = Reprojector.reprojectGeometry(AOI.first().getGeometry, targetMetadata.srid) // calling long time
    val lineMBR: Envelope = geom.getEnvelopeInternal
    targetMetadata.modelToGrid(lineMBR.getMinX, lineMBR.getMinY, corner1)
    targetMetadata.modelToGrid(lineMBR.getMaxX, lineMBR.getMaxY, corner2)
    val i1 = Math.max(0, Math.min(corner1.x, corner2.x))
    val i2 = Math.min(targetMetadata.rasterWidth, Math.ceil(Math.max(corner1.x, corner2.x)))
    val j1 = Math.max(0, Math.min(corner1.y, corner2.y))
    val j2 = Math.min(targetMetadata.rasterHeight, Math.ceil(Math.max(corner1.y, corner2.y)))

    targetMetadata.gridToModel(i1, j1, corner1)
    targetMetadata.gridToModel(i2, j2, corner2)
    val x1 = Math.min(corner1.x, corner2.x)
    val y1 = Math.min(corner1.y, corner2.y)
    val x2 = Math.max(corner1.x, corner2.x)
    val y2 = Math.max(corner1.y, corner2.y)

    val cellSizeX = targetMetadata.getPixelScaleX
    val rasterW = Math.floor((x2 - x1) / cellSizeX).toInt
    val rasterH = Math.floor((y2 - y1) / cellSizeX).toInt

    val targetMetadataGeom = RasterMetadata.create(lineMBR.getMinX, lineMBR.getMaxY, lineMBR.getMaxX, lineMBR.getMinY, targetMetadata.srid, rasterW, rasterH, 256, 256)
    targetMetadataGeom
  }

  private def listDateFolders(basePath: Path, rasterFileSystem: FileSystem): Array[(String, String)] = {
    val logger = LoggerFactory.getLogger(getClass)
    val dateRegex = raw"\d{4}-\d{2}-\d{2}".r
    val folderDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    try {
      if (!rasterFileSystem.exists(basePath) || !rasterFileSystem.getFileStatus(basePath).isDirectory) {
        logger.error(s"Base path does not exist or is not a directory: $basePath")
        return Array.empty[(String, String)]
      }

      val subDirs = rasterFileSystem.listStatus(basePath).filter(_.isDirectory).flatMap { dirStatus =>
        val folderName = dirStatus.getPath.getName.trim
        folderName match {
          case dateRegex() =>
            try {
              val parsedDate = LocalDate.parse(folderName, folderDateFormatter)
              val monthDay = parsedDate.format(DateTimeFormatter.ofPattern("MM-dd"))
              logger.info(s"Found date folder: $folderName -> MM-dd: $monthDay")
              Some((monthDay, dirStatus.getPath.toString))
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

      if (subDirs.isEmpty) {
        logger.warn(s"No date-formatted folders found under: $basePath")
      }

      subDirs
    } catch {
      case e: Exception =>
        logger.error(s"Error while listing folders under: $basePath", e)
        Array.empty[(String, String)]
    }
  }

  def findLandsatFilesForTwoMonthWindowPaths(
                                              targetDate: LocalDate,
                                              baseFolder: String,
                                              thresholdDays: Int,
                                              hadoopConf: Configuration
                                            ): Map[LocalDate, Seq[String]] = {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val startDate = targetDate.minusMonths(1)
    val endDate = targetDate.plusMonths(1)
    val fs = FileSystem.get(hadoopConf)
    val days = Iterator.iterate(startDate)(_.plusDays(1)).takeWhile(!_.isAfter(endDate)).toList
    val data = days.flatMap { date =>
      val folderPath = new Path(s"$baseFolder/${date.format(dateFormatter)}")
      if (fs.exists(folderPath) && fs.getFileStatus(folderPath).isDirectory) {
        val tiffs = fs.listStatus(folderPath)
          .filter(s => s.isFile && s.getPath.getName.toLowerCase.endsWith(".tif"))
          .map(_.getPath.toString).toSeq
        if (tiffs.nonEmpty) Some(date -> tiffs) else None
      } else None
    }.toMap
    if (data.size < thresholdDays)
      println(s"Warning: only found ${data.size} days in window $startDate to $endDate")
    data
  }

  //def extractMonthDay(ts: Timestamp): String = ts.toLocalDateTime.format(mdFmt)
  def extractMonthDay(date: LocalDate): String = date.format(mdFmt)


  def toDummyTimestamp(dateStr: String): Timestamp = {
    val parts = dateStr.split("-")
    val dt = LocalDateTime.parse(
      s"1970 ${parts(1)} ${parts(2)} 0000",
      DateTimeFormatter.ofPattern("yyyy MM dd HHmm")
    )
    Timestamp.valueOf(dt)
  }
  def processDay(
                  sc: SparkContext,
                  datein: LocalDate,
                  landsatDate: LocalDate,
                  aoi: RDD[IFeature],
                  metadata: RasterMetadata,
                  fs: FileSystem,
                  numParts: Int,
                  part: Partitioner,
                  prevOpt: Option[RDD[(Int, ITile[Array[Float]])]],
                  soilAWCKV: RDD[(Int, ITile[Float])],
                  soilFCKV: RDD[(Int, ITile[Float])],
                  nlcdKV: RDD[(Int, ITile[Float])],
                  elevKV: RDD[(Int, ITile[Float])]
                ): RDD[(Int, ITile[Array[Float]])] = {
    // PRISM
    //val prismPath = listDateFolders(new Path("PRISM_2023_Geotiff"), fs).find(_._1==extractMonthDay(date)).get._2
    val startDaytime = System.nanoTime()
    val targetMonthDay = extractMonthDay(datein)
    val prism_daily = "PRISM_2023_Geotiff" // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
    val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration)) //ListDate_daily_hourly(prism_daily)
    val maybeFolderPrism: Option[String] = PRISM_filter
      .find { case (monthDay, _) => monthDay == targetMonthDay }
      .map(_._2)
    val prismKV = enforcePartitionKV(
      RasterOperationsFocal.reshapeNN(sc.geoTiff[Float](maybeFolderPrism.get), _ => metadata, numParts), part)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    prismKV.foreachPartition(_ => ())

    // Landsat ±1m
    val b4Map = findLandsatFilesForTwoMonthWindowPaths(landsatDate, "California_Landsat8_4B_2021/B4", 10, sc.hadoopConfiguration)
    val b5Map = findLandsatFilesForTwoMonthWindowPaths(landsatDate, "California_Landsat8_4B_2021/B5", 10, sc.hadoopConfiguration)
    val b4RDD: RasterRDD[Int] = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
    val b5RDD: RasterRDD[Int] = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
    val rs4: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(b4RDD, _ => metadata, numParts)
    val rs5: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(b5RDD, _ => metadata, numParts)
    val lsOv: RasterRDD[Array[Int]] = RasterOperationsLocal.overlay(rs4, rs5)
    val lsOv_notnull_val = lsOv.mapPixels(p => {
      val ndvi = (p(1) - p(0)).toFloat / (p(1) + p(0)+0.000001f).toFloat
      val lai = new CalculationLAI().ndviLaiFunc(ndvi)
      Array(ndvi,lai)
    })
    val landsKV = enforcePartitionKV(
      lsOv_notnull_val, part, sort = true).persist(StorageLevel.MEMORY_AND_DISK_SER)
    landsKV.foreachPartition(_ => ())

    // Static+PRISM
    val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
      .persist(StorageLevel.DISK_ONLY)
    baseKV.foreachPartition(_ => ())

    // --- Load today's NLDAS files ---
    val dateStr = datein.toString
    val dayDir  = new Path(s"aoidata_nldas_all/$dateStr")
    val tifPaths = fs
      .listStatus(dayDir)
      .filter(_.getPath.getName.toLowerCase.endsWith(".tif"))
      .map(_.getPath.toString)
      .sorted // ensure chronological
    // rolling checkpoint GeoTiff path
    val ckptGeoTiff = s"hdfs:///user/zshan011/ckpt/$dateStr/latest.tif"
    // ensure parent dir
    val ckptDir = new Path(ckptGeoTiff).getParent
    if (!fs.exists(ckptDir)) fs.mkdirs(ckptDir)

    // replay your "processDay" inner logic for each batch:
    var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull
    var hour = 0
    // prepare a buffer to collect per‑hour timings
    val hourTimings = ListBuffer[Double]()
    // prepare base HDFS path for today’s checkpoints
//    val baseCkptDir = new Path(s"hdfs:///user/zshan011/ckpt/$dateStr")
//    if (!fs.exists(baseCkptDir)) fs.mkdirs(baseCkptDir)

    tifPaths.zipWithIndex.foreach { case (path, idx) =>
      println(s"▶︎ Starting hour ${idx+1} – reshapeNN for $path")
      val t0 = System.nanoTime()

      // 1) read & reshape this hour
      val hrTiles: RDD[ITile[Array[Float]]] =
        RasterOperationsFocal
          .reshapeNN(sc.geoTiff[Array[Float]](path), _ => metadata, numParts)
      val hrKV: RDD[(Int, ITile[Array[Float]])] =
        hrTiles
          .map(t => (t.tileID, t))
          .partitionBy(part).mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning=true)
//        enforcePartitionKV(
//        RasterOperationsFocal.reshapeNN(sc.geoTiff[Array[Float]](path), _ => metadata, numParts),
//        part, sort = true
//      )

      val layered   = if (cycle == null) RasterOperationsLocal.overlayArrayStreamsF2(baseKV, hrKV, landsKV) else RasterOperationsLocal.overlayArrayStreamsF2(baseKV, hrKV, landsKV, cycle)
      // 3) iterative ET calculation
      val rawNext = BandCalculationCheck2.IterativeCalculationPre2(layered, null)

      // 4) shrink every tile to exactly 5 bands (fill with -9999f if missing)
      val next5: RDD[(Int, ITile[Array[Float]])] =
        RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(
          rawNext,
          (p: Array[Float]) => Array(
            if (p.length > 0) p(0) else -9999f,
            if (p.length > 1) p(1) else -9999f,
            if (p.length > 2) p(2) else -9999f,
            if (p.length > 3) p(3) else -9999f,
            if (p.length > 4) p(4) else -9999f
          )
        )
      // 5) materialize
      next5.persist(StorageLevel.DISK_ONLY)
      next5.foreachPartition(_ => ())

      // 6) drop old state
      hrKV.unpersist(false)
      layered.unpersist(false)
      rawNext.unpersist(false)
      if (cycle != null) cycle.unpersist(false)

      // 7) every checkpointInterval hours, write/overwrite GeoTiff & reload
      if ((idx + 1) % checkpointInterval == 0) {
        // delete old
        val ck = new Path(ckptGeoTiff)
        if (fs.exists(ck)) fs.delete(ck, true)
        // write one 5-band GeoTiff
        GeoTiffWriter.saveAsGeoTiff(
          next5.values, // RasterRDD[Array[Float]]
          ckptGeoTiff,
          Seq(
            GeoTiffWriter.WriteMode -> "compatibility",
            GeoTiffWriter.BigTiff -> "yes")
        )
        // drop it
        next5.unpersist(blocking = true)
        // reload, re-key, re-partition, re-persist
        cycle = sc.geoTiff[Array[Float]](ckptGeoTiff)
          .map(t => (t.tileID, t))
          .partitionBy(part)
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        cycle.count()
      } else {
        // no checkpoint this hour, keep next5 in memory
        cycle = next5
      }
      // record timing
      val elapsed = (System.nanoTime() - t0) / 1e9
      hourTimings += elapsed
      println(f"✅ Hour ${idx + 1}%2d done in $elapsed%6.2f sec")
    }
    landsKV.unpersist(blocking = true)
    baseKV.unpersist(blocking = true)
    prismKV.unpersist(blocking = true)
    // end of day summary
    println("\n=== Daily Breakdown ===")
    hourTimings.zipWithIndex.foreach { case (t, i) =>
      println(f" Hour ${i + 1}%2d: $t%6.2f sec")
    }
    val tot = hourTimings.sum
    println(f" — total: $tot%6.2f sec, avg: ${tot / hourTimings.size}%6.2f sec/hr\n")
    cycle

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MultiTemporalET").setIfMissing("spark.master", "local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    //sc.setCheckpointDir("hdfs:///user/zshan011/landsat_ckpt")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val startTime = System.nanoTime()

    // AOI & metadata with two-month sample period
    val aoi = sc.shapefile("LA/POLYGON.shp")
    // sample Landsat B4 over ±1 month around the start date
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
    val parts = sample.getNumPartitions //* 2
    val part = new HashPartitioner(parts)

    // Static yearly layers
    val elevRDD = sc.geoTiff[Float]("NLDAS_Elevation_2023_Geotiff")
    val awcRDD = sc.geoTiff[Float]("Soil_2023/awc_gNATSGO_US.tif")
    val fcRDD = sc.geoTiff[Float]("Soil_2023/fc_gNATSGO_US.tif")
    val nlcdInt = sc.geoTiff[Int]("NLCD_2023")
    val nlcdRDD = nlcdInt.mapPixels(_.toFloat)
    val elevKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(elevRDD, _ => aoiMeta, parts), part)
      .persist(StorageLevel.DISK_ONLY)
    elevKV.foreachPartition(_ => ())

    val awcKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(awcRDD, _ => aoiMeta, parts), part)
      .persist(StorageLevel.DISK_ONLY)
    awcKV.foreachPartition(_ => ())

    val fcKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(fcRDD, _ => aoiMeta, parts), part)
      .persist(StorageLevel.DISK_ONLY)
    fcKV.foreachPartition(_ => ())

    val nlcdKV = enforcePartitionKV(RasterOperationsFocal.reshapeNN(nlcdRDD, _ => aoiMeta, parts), part)
      .persist(StorageLevel.DISK_ONLY)
    nlcdKV.foreachPartition(_ => ())

    // Loop over days
    var curr = LocalDate.parse("2023-01-01")
    val end = LocalDate.parse("2023-01-07")
    // start/end for your Landsat sampling window (±1 month around this date):
    var currLandsat = LocalDate.parse("2021-01-01")
    val endLandsat = LocalDate.parse("2021-01-07")
    var star = curr.toString
    val endDate = end.toString
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]] = None
    while (!curr.isAfter(end)) {
      prevOpt = Some(processDay(sc, curr, currLandsat, aoi, aoiMeta, fs, parts, part, prevOpt, awcKV, fcKV, nlcdKV, elevKV))
      curr = curr.plusDays(1)
      currLandsat = currLandsat.plusDays(1)
    }

    // Save final composite
    val finalRdd: RasterRDD[Array[Float]] = prevOpt.get.values.mapPixels(arr => {
      val out = Array.fill(5)(-9999f)
      Array.copy(arr, 0, out, 0, math.min(arr.length, 5))
      out
    })
    val output = "aoidata_rdpro_landsat_la"
    val output_file = s"$output/landsat-$star-$endDate.nn.tif"
    GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.OutputNumBands -> "5", GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    val endTime = System.nanoTime()
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
    spark.stop()
  }
}
