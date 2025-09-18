package com.baitsss.model

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.{RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal, StackedTile}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Dependency, HashPartitioner, OneToOneDependency, Partition, Partitioner, ShuffleDependency, SparkConf, SparkEnv}
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.slf4j.LoggerFactory

import java.awt.geom.Point2D
import java.io.File
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.annotation.varargs
import scala.reflect.ClassTag

object CoGroup_test {
  @transient lazy val log = Logger.getLogger(getClass.getName)

//  def sortTilesByID[Array[Float]](rdd: RasterRDD[Array[Float]]): RasterRDD[Array[Float]] = {
//    rdd.map(t => (t.tileID, t)) // (Int, ITile)
//      .mapPartitions(iter => iter.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
//      .map(_._2)
//      .asInstanceOf[RasterRDD[Array[Float]]]
//  }

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

  def ListDate_daily_hourly(basePath: String): Array[(Timestamp, String)] = {
    val baseDir = new File(basePath)

    // Check if the base directory exists and is a directory
    if (!baseDir.exists() || !baseDir.isDirectory) {
      throw new IllegalArgumentException(s"Path '$basePath' does not exist or is not a directory.")
    }

    // List subfolders
    val subDirs = baseDir.listFiles().filter(_.isDirectory)

    // Regex for matching date folders in "yyyy-MM-dd" format
    val dateRegex = """\d{4}-\d{2}-\d{2}""".r

    // Prepare a formatter for the dummy datetime string "yyyy MM dd HHmm"
    val dummyFormatter = DateTimeFormatter.ofPattern("yyyy MM dd HHmm")

    // Map each valid date subfolder to (Timestamp, fullFolderPath)
    val results = subDirs.flatMap { dir =>
      val folderName = dir.getName // e.g., "2023-01-01"
      folderName match {
        case dateRegex() =>
          // Split the folder name by "-" to extract month/day
          val parts = folderName.split("-") // e.g. Array("2023","01","01")
          val dummyYear = "1970"
          val monthPart = parts(1) // "01"
          val dayPart = parts(2) // "01"
          val dummyHour = "0000"

          // Build a new datetime string: "1970 01 01 0000"
          val dummyDateStr = s"$dummyYear $monthPart $dayPart $dummyHour"
          val dateTime = LocalDateTime.parse(dummyDateStr, dummyFormatter)
          val timestamp = Timestamp.valueOf(dateTime)
          println("planet: ", timestamp)
          // Return a tuple with the dummy timestamp and the full path to the subfolder
          Some((timestamp, dir.getAbsolutePath))

        case _ =>
          // Not a valid date folder, skip it
          None
      }
    }
    results
    // Sort the results by timestamp (if desired)
    //results.sortBy(_._1)
  }

  // Function to extract MM-dd part from Timestamp for comparison
  def extractMonthDay(timestamp: Timestamp): String = {
    val dateTime = timestamp.toLocalDateTime
    dateTime.format(DateTimeFormatter.ofPattern("MM-dd"))
  }

  // Function to extract MM-dd part from LocalDate for comparison
  def extractMonthDay(date: LocalDate): String = {
    date.format(DateTimeFormatter.ofPattern("MM-dd"))
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

  def alignPartitioners[T: ClassTag](
                                      rdds: Seq[RDD[ITile[T]]],
                                      numPartitions: Int
                                    ): Seq[RDD[ITile[T]]] = {
    val partitioner = new HashPartitioner(numPartitions)
    rdds.map { rdd =>
      rdd.map(tile => (tile.tileID, tile))
        .partitionBy(partitioner)
        .mapPartitions(_.map(_._2), preservesPartitioning = true)
    }
  }

  def getDateTimestamp(inputDate: String): Timestamp = {
    // Split the input date string to extract month and day.
    val tokens = inputDate.split("-")
    val month = tokens(1) // e.g., "01"
    val day = tokens(2) // e.g., "01"

    // Use a fixed dummy year and fixed hour.
    val dummyYear = "1970"
    val dummyHour = "0000"

    // Build a new datetime string in the format "yyyy MM dd HHmm".
    // This will produce, for example, "1970 01 01 0000".
    val newDateTimeStr = s"$dummyYear $month $day $dummyHour"

    // Define a DateTimeFormatter matching "yyyy MM dd HHmm"
    val formatter = DateTimeFormatter.ofPattern("yyyy MM dd HHmm")

    // Parse the new datetime string into a LocalDateTime and then convert to a Timestamp.
    val dateTime = LocalDateTime.parse(newDateTimeStr, formatter)
    val startTimestamp = Timestamp.valueOf(dateTime)
    startTimestamp
  }

  /** Returns true iff the first RDD has a defined partitioner and
   * every other RDD has exactly the same one. */
  /** Returns true iff the first RDD has a defined partitioner and
   * every other RDD has exactly the same one. */
  private def samePartitioning[T](rdds: Seq[RDD[T]]): Boolean = rdds match {
    case first +: rest if first.partitioner.isDefined =>
      rest.forall(_.partitioner == first.partitioner)
    case _ => false
  }

  /**
   * If all inputs share the same partitioner, returns inputs.head (no shuffle).
   * Otherwise builds and returns a CoGroupedRDD (which will shuffle).
   * Logs at each step so you can see exactly what’s happening.
   */
  def overlayCheck[T: ClassTag](@varargs inputs: RDD[ITile[T]]*): RDD[_] = {
    require(inputs.nonEmpty, "overlayCheck needs at least one input RDD")

    val willShuffle = !(inputs.size >= 2 && samePartitioning(inputs))
    println(s"willShuffle = $willShuffle")
    log.info(s"willShuffle = $willShuffle")      // <— goes into the Spark logs

    if (!willShuffle) {
      println("co-partitioned → no shuffle. Returning inputs.head.")
      inputs.head

    } else {
      println(" ot co-partitioned → building CoGroupedRDD (this shuffles).")
      // turn each ITile[T] RDD into (tileID, tile)
      val byKey: Seq[RDD[(Int, ITile[T])]] = inputs.map(_.map(t => (t.tileID, t)))
      // derive or reuse a partitioner
      val partitioner = Partitioner.defaultPartitioner(byKey.head, byKey: _*)
      // build the cogroup-RDD
      val cg = new CoGroupedRDD[Int](byKey, partitioner)

      // extra logging:
      println(s"   Partitioner on CoGroupedRDD: ${cg.partitioner}")
      println(s"   Has shuffle dependency? ${cg.dependencies.exists(_.isInstanceOf[ShuffleDependency[_,_,_]])}")

      cg
    }
  }
  import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
  import scala.collection.mutable

  def main(args: Array[String]): Unit = {
    // --- Spark session initialization ---
    val conf = new SparkConf().setAppName("MultiTemporalDataAggregator")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val listener = new StageCountListener()
    sc.addSparkListener(listener)
    val progress = sc.longAccumulator("hourlyProgress")
    sc.setCheckpointDir("landsat_checkpoints")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val startTime = System.nanoTime()
    // --- STEP 0: Install a tiny SparkListener to record each stage’s runtime ---
    val stageTimes = mutable.Map[Int, (Long, Long)]() // stageId → (launchTime, finishTime)
    sc.addSparkListener(new SparkListener {
      override def onStageCompleted(stage: SparkListenerStageCompleted): Unit = {
        val info = stage.stageInfo
        stageTimes(info.stageId) = (info.submissionTime.get, info.completionTime.get)
      }
    })
    // --- Define input directories ---

    // AOI
    val AOI_shp = "/Users/clockorangezoe/Downloads/CA/CA.shp"
    val AOI:RDD[IFeature] = sc.shapefile(AOI_shp)
    val targetCRS = CRS.decode("EPSG:4326") //4326, 3857

    // Define start and end dates.
    var count = 0

    val wholePathStringB4 = "/Users/clockorangezoe/Desktop/BAITSSS_project/data/landsat8_cali/B4"
    val Landsat_RDD_B4: RasterRDD[Int] = sc.geoTiff(wholePathStringB4)
    val Landsat_metadata = Landsat_RDD_B4.first().rasterMetadata
    val Landsat_all_metadata = RasterMetadata.allMetadata(Landsat_RDD_B4)
    val Landsat_RDD_Metadata = Raster_metadata(Landsat_metadata, Landsat_all_metadata, targetCRS)
    val targetMetadataGeom = AOI_metadata(AOI, Landsat_RDD_Metadata)
    val landsat_RDD_B4_reshape: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(Landsat_RDD_B4, RasterMetadata => targetMetadataGeom)
    val Landsat: RasterRDD[Array[Float]] = RasterOperationsLocal.mapPixels(landsat_RDD_B4_reshape, (p: Int) => {
        val value = p
        val LAI = new CalculationLAI().ndviLaiFunc(value)
        Array(value, LAI.toFloat)
      })
    val targetNumPartition = Landsat.getNumPartitions
    val part = new HashPartitioner(targetNumPartition)

    val NLDAS = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/NLDAS2019_Geotiff_test0/2019-09-23"
    val NLDAS_RDD_curr: RasterRDD[Array[Float]] = sc.geoTiff(NLDAS)
    val NLDAS_obtain_values = NLDAS_RDD_curr.mapPixels(p => {
      val values = new Array[Float](4)
      values(0) = p(0) // Tair_oC
      values(1) = p(1) // S_hum
      //wind_array = np.sqrt(wind_u_array ** 2 + wind_v_array ** 2)
      val wind_array = Math.sqrt(p(3) * p(3) + p(4) * p(4)) //uz
      values(2) = wind_array.toFloat
      values(3) = p(5) // In_short
      values
    })
    val upsampling = RasterOperationsFocal.reshapeNN(NLDAS_obtain_values, RasterMetadata => targetMetadataGeom, targetNumPartition)
    // keyBy + partitionBy gives you a PairRDD[K, V] with .partitioner = Some(part)
    val pairA: RDD[(Int, ITile[Array[Float]])] = Landsat.keyBy(_.tileID).partitionBy(part).asInstanceOf[RDD[(Int, ITile[Array[Float]])]]
    val pairB: RDD[(Int, ITile[Array[Float]])] = upsampling.keyBy(_.tileID).partitionBy(part)
//
//    // .values() preserves the upstream partitioner!
    //val A2 = pairA.values
    //val B2 = pairB.values
    var nextroundcheck: RDD[(Int,ITile[Array[Float]])]  = null

    val overlaycheck = RasterOperationsLocal.overlayArrayStreamsF2(pairA, pairB)
    val overlaynormal: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(Landsat, upsampling)
    nextroundcheck = RasterOperationsLocal.mapPixelsPreservePartitionerKeyed( overlaycheck, (p:Array[Float]) => {
      Array(
              if (p.length > 0) p(0) else -9999f,
              if (p.length > 1) p(1)+p(0) else -9999f,
              if (p.length > 2) p(2)/p(1) else -9999f,
              if (p.length > 3) p(3)+p(2) else -9999f,
              if (p.length > 4) p(4)-p(0) else -9999f
            )
    })
    val nextroundnormal = RasterOperationsLocal.mapPixels(overlaynormal, (p: Array[Float]) => {
      Array(
        if (p.length > 0) p(0) else -9999f,
        if (p.length > 1) p(1) + p(0) else -9999f,
        if (p.length > 2) p(2) / p(1) else -9999f,
        if (p.length > 3) p(3) + p(2) else -9999f,
        if (p.length > 4) p(4) - p(0) else -9999f
      )
    })
    //val pairC: RDD[(Int, ITile[Array[Float]])] = nextroundcheck.keyBy(_.tileID).partitionBy(part)

    val overlaycheck2 = RasterOperationsLocal.overlayArrayStreamsF2(pairA,pairB,nextroundcheck)
    val overlaynormal2 = RasterOperationsLocal.overlay(Landsat,upsampling,nextroundnormal)


    // STEP 1: Cache both so we only pay the cost once each
//    overlaycheck2.cache()
//    overlaynormal2.cache()

    // STEP 2: Print plans
    println("=== zipPreserve plan ===")
    println(overlaycheck2.toDebugString)
    println("=== normal overlay plan ===")
    println(overlaynormal2.toDebugString)

    // STEP 3: Helper to time a block
    def time[R](block: => R): (R, Double) = {
      val t0 = System.nanoTime()
      val r = block
      val t1 = System.nanoTime()
      (r, (t1 - t0) / 1e9)
    }

    // STEP 4: Count + time
    val (cntZip, tz) = time {
      overlaycheck2.count()
    }
    val (cntNorm, tn) = time {
      overlaynormal2.count()
    }

    println(f"\nResults: zipPreserve count=$cntZip time=${tz}%2.3f s")
    println(f"         normal   count=$cntNorm time=${tn}%2.3f s")

    // STEP 5: Print per-stage durations
    println("\n=== Stage Durations (seconds) ===")
    stageTimes.toSeq.sortBy(_._1).foreach { case (id, (s, e)) =>
      println(f"Stage $id: ${(e - s) / 1e3}%2.3f s")
    }

    val output_file = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/aoidata_rdpro"
    val outputRDDLazy = overlaycheck.values.mapPixels { p =>
      Array(
        if (p.length > 0) p(0) else -9999f,
        if (p.length > 1) p(1) else -9999f,
        if (p.length > 2) p(2) else -9999f,
        if (p.length > 3) p(3) else -9999f,
        if (p.length > 4) p(4) else -9999f
      )
    }
    val outputRDDNormal = overlaynormal.mapPixels { p =>
      Array(
        if (p.length > 0) p(0) else -9999f,
        if (p.length > 1) p(1) else -9999f,
        if (p.length > 2) p(2) else -9999f,
        if (p.length > 3) p(3) else -9999f,
        if (p.length > 4) p(4) else -9999f
      )
    }
    GeoTiffWriter.saveAsGeoTiff(outputRDDLazy, output_file+"/lazyCA.tif", Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    GeoTiffWriter.saveAsGeoTiff(outputRDDNormal, output_file+"/normaCA.tif", Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"

  }
}
