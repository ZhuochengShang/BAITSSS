package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel_listFile_disk.listDateFolders
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.slf4j.LoggerFactory

import java.awt.geom.Point2D
import java.io.File
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.reflect.ClassTag

object PipelineLandsatETModel_listFile_disk2 {

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

  def main(args: Array[String]): Unit = {
    // --- Spark session initialization ---
    val conf = new SparkConf().setAppName("MultiTemporalDataAggregator")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val listener = new StageCountListener()
    sc.addSparkListener(listener)
    val progress = sc.longAccumulator("hourlyProgress")

    val startTime = System.nanoTime()

    // --- Define input directories ---
    val output = "aoidata_rdpro_landsat_ca"
    // Define start and end dates.
    val startDate = LocalDate.parse("2023-01-01")
    val endDate = LocalDate.parse("2023-01-01")
    var currentDate = startDate
    // Define your desired window: if NLDAS key indicates day "01", then window is day 1 to day 16.
    val NLDAS = "NLDAS_2023_Geotiff"
    val NLDAS_filter = listDateFolders(new Path(NLDAS), FileSystem.get(sc.hadoopConfiguration))

    var previousCycle: RasterRDD[Array[Float]] = null
    while (!currentDate.isAfter(endDate)) {
      val dateStr = currentDate.toString  // e.g., "2023-01-01"
      val targetMonthDay = extractMonthDay(currentDate) // Extract "MM-dd" from current date
      val startTimestamp = getDateTimestamp(dateStr)
      println(s"Constructed datetime string: ", startTimestamp)

      val single_path = s"${output}_single/static_overlay_$dateStr.tif"
      val landsat_path = s"${output}_landsat/landsat_ndvi_lai_$dateStr.tif"
      val single_bans_overlay_in: RasterRDD[Array[Float]] = sc.geoTiff(single_path).persist(StorageLevel.DISK_ONLY)
      val landsat_in: RasterRDD[Array[Float]] = sc.geoTiff(landsat_path).persist(StorageLevel.DISK_ONLY)

      val targetMetadataGeom = landsat_in.first().rasterMetadata
      val targetNumPartition = landsat_in.getNumPartitions

      val maybeFolderNLDAS: Option[String] = NLDAS_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      val NLDAS_file = maybeFolderNLDAS.getOrElse {
        throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
      }
      val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray
      var hour = 0
      NLDAS_RasterRDD.foreach(t => {
        // val timeStamp = t._1
        val filename = t._2
        println(filename)
        val NLDAS_RDD_curr: RasterRDD[Array[Float]] = sc.geoTiff(filename)
        // map as float32
        //val NLDAS_Float: RasterRDD[Array[Float]] = NLDAS_RDD_curr.mapPixels(p => p.map(_.toFloat))
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
        // (timeStamp, upsampling)
        if (previousCycle == null) {
          val layer3: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(single_bans_overlay_in, upsampling, landsat_in) //.asInstanceOf[RasterRDD[Array[Float]]
          previousCycle = BandCalculationCheck2.IterativeCalculationPre(layer3, null)
        } else {
          val layer3: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(single_bans_overlay_in, upsampling, landsat_in, previousCycle) //.asInstanceOf[RasterRDD[Array[Float]]
          // pixel calculate
          // initial previous tile as empty tile
          if (previousCycle != null) previousCycle.unpersist()
          previousCycle = BandCalculationCheck2.IterativeCalculationPre(layer3, null)
        }
        hour += 1
        progress.add(1)
        if (progress.value % 4 == 0) println(s"Hourly progress: ${progress.value}/24 done")
      })// end 24 hours
      // Increment to the next day.
      currentDate = currentDate.plusDays(1)
      landsat_in.unpersist()
      single_bans_overlay_in.unpersist()
    }
    val prevfiLTER = previousCycle.filterPixels(_.length == 4)
    val output_file = s"$output/landsat-$startDate-$endDate.nn.tif"
    GeoTiffWriter.saveAsGeoTiff(prevfiLTER, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_DEFLATE, GeoTiffWriter.OutputNumBands -> "4", GeoTiffWriter.BigTiff ->"yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    val endTime = System.nanoTime()
    println(s"Total stages executed: ${listener.stageCount}")
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
    spark.stop()
  }
}
