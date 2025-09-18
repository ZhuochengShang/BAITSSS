package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel.listDateFolders
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.util.Parallel2
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.slf4j.LoggerFactory

import java.awt.geom.Point2D
import java.io.File
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.reflect.ClassTag

object PipelineLandsatETModel_listFile_all {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def sortTilesByID[T](rdd: RasterRDD[T]): RasterRDD[T] = {
    rdd.map(t => (t.tileID, t)) // (Int, ITile)
      .mapPartitions(iter => iter.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
      .map(_._2)
      .asInstanceOf[RasterRDD[T]]
  }

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
//    conf.set("spark.kryo.registrator", "your.kryo.Registrator")
//      .set("spark.kryoserializer.buffer.max", "2000m")
//      .set("spark.io.compression.codec", "lz4")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val listener = new StageCountListener()
    sc.addSparkListener(listener)
    val progress = sc.longAccumulator("hourlyProgress")
    sc.setCheckpointDir("landsat_checkpoints")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val startTime = System.nanoTime()
    // --- Define input directories ---
    val output = "aoidata_rdpro_landsat_ca"

    // AOI
    val AOI_shp = "CA/CA.shp"
    val AOI:RDD[IFeature] = sc.shapefile(AOI_shp)
    val targetCRS = CRS.decode("EPSG:4326") //4326, 3857

    val elevation_yearly = "NLDAS_Elevation_2023_Geotiff"
    val soil_yearly_awc = "Soil_2023/awc_gNATSGO_US.tif" // Contains one yearly file, e.g., NLDAS_YEARLY.A2023.002.tif
    val soil_yearly_fc = "Soil_2023/fc_gNATSGO_US.tif" // Contains one yearly file, e.g., NLDAS_YEARLY.A2023.002.tif
    val nlcd_yearly = "NLCD_2023"

    val prism_daily = "PRISM_2023_Geotiff" // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
    val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration)) //ListDate_daily_hourly(prism_daily)

    val Landsat_daily_B4 = "California_Landsat8_4B_2021/B4"
    val Landsat_daily_B5 = "California_Landsat8_4B_2021/B5"

    // Define start and end dates.
    val startDate = LocalDate.parse("2023-01-01")
    val endDate = LocalDate.parse("2023-01-01")
    val startDate_landsat = LocalDate.parse("2021-01-01")
    val endDate_landsat = LocalDate.parse("2021-01-01")
    var currentDate = startDate
    var currentDate_landsat = startDate_landsat

    val matchingLandsatFolders_B4: Map[LocalDate, Seq[String]] = new LoadLandsat().findLandsatFilesForTwoMonthWindowPaths(currentDate_landsat, Landsat_daily_B4, 10, sc.hadoopConfiguration)
    val filesMap_B4: Array[String] = matchingLandsatFolders_B4.toSeq
      .flatMap { case (_, paths) => paths }
      .toArray
    // Combine all file paths into one whole string separated by spaces.
    val wholePathStringB4: String = filesMap_B4.mkString(" ")
    println(wholePathStringB4)
    println(s"Matching Landsat B4 folders: $wholePathStringB4")

    val matchingLandsatFolders_B5: Map[LocalDate, Seq[String]] = new LoadLandsat().findLandsatFilesForTwoMonthWindowPaths(currentDate_landsat, Landsat_daily_B5, 10, sc.hadoopConfiguration)
    // Flatten the Map: sort by date (if desired), then extract just the file paths.
    val filesMap_B5: Array[String] = matchingLandsatFolders_B5.toSeq
      .flatMap { case (_, paths) => paths }
      .toArray
    // Combine all file paths into one whole string separated by spaces.
    val wholePathStringB5: String = filesMap_B5.mkString(" ")
    println(wholePathStringB5)

    val Landsat_RDD_B4: RasterRDD[Int] = sc.geoTiff(wholePathStringB4)
    val Landsat_RDD_B5: RasterRDD[Int] = sc.geoTiff(wholePathStringB5)

    val Landsat_metadata = Landsat_RDD_B4.first().rasterMetadata
    val Landsat_all_metadata = RasterMetadata.allMetadata(Landsat_RDD_B4)
    val Landsat_RDD_Metadata = Raster_metadata(Landsat_metadata, Landsat_all_metadata, targetCRS)
    val targetMetadataGeom = AOI_metadata(AOI, Landsat_RDD_Metadata)
    val landsat_RDD_B4_reshape: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(Landsat_RDD_B4, RasterMetadata => targetMetadataGeom)
    val landsat_RDD_B5_reshape: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(Landsat_RDD_B5, RasterMetadata => targetMetadataGeom)
    val LandsatOverlay: RasterRDD[Array[Int]] = RasterOperationsLocal.overlay(landsat_RDD_B4_reshape, landsat_RDD_B5_reshape)
    val Landsat: RasterRDD[Array[Float]] = RasterOperationsLocal.mapPixels(LandsatOverlay, (p: Array[Int]) => {
      val value = ((p(1) - p(0)).toFloat / (p(1) + p(0)).toFloat).toFloat
      var pixelValue = value
      if (value > 1) {
        pixelValue = 1
      } else if (value < -1) {
        pixelValue = -1
      }
      val LAI = new CalculationLAI().ndviLaiFunc(value)
      Array(pixelValue, LAI.toFloat)
    })
    val targetNumPartition = Landsat.getNumPartitions
    val part = new HashPartitioner(targetNumPartition)

    def enforcePartitioner(rdd: RDD[ITile[Array[Float]]]): RDD[(Int, ITile[Array[Float]])] = {
      // key by your tileID, partition, then drop the key
      rdd
        .keyBy(_.tileID) // PairRDD[(TileID,ITile)]
        .partitionBy(part) // now every partitioner is the same
    }

    def enforcePartitionerAndSort(
                                   rdd: RDD[ITile[Array[Float]]],
                                   partitioner: Partitioner
                                 ): RDD[(Int, ITile[Array[Float]])] = {
      rdd
        .keyBy(_.tileID)
        .partitionBy(partitioner)
        .mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
    }

    def enforcePartitioner2(rdd: RDD[ITile[Float]]): RDD[(Int, ITile[Float])] = {
      // key by your tileID, partition, then drop the key
      rdd
        .keyBy(_.tileID) // PairRDD[(TileID,ITile)]
        .partitionBy(part) // now every partitioner is the same
    }

    def enforcePartitionerAndSort2(
                                   rdd: RDD[ITile[Float]],
                                   partitioner: Partitioner
                                 ): RDD[(Int, ITile[Float])] = {
      rdd
        .keyBy(_.tileID)
        .partitionBy(partitioner)
        .mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
    }

    val ELEV_RDD: RasterRDD[Float] = sc.geoTiff(elevation_yearly)
    val SOIL_AWC_RDD: RasterRDD[Float] = sc.geoTiff(soil_yearly_awc)//.persist(StorageLevel.DISK_ONLY)
    val SOIL_FC_RDD: RasterRDD[Float] = sc.geoTiff(soil_yearly_fc)//.persist(StorageLevel.DISK_ONLY)
    val NLCD_RDD: RasterRDD[Int] = sc.geoTiff(nlcd_yearly)
    val NLCD_RDD_Float = NLCD_RDD.mapPixels(_.toFloat)//.persist(StorageLevel.DISK_ONLY)

    val ELEV_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(ELEV_RDD, RasterMetadata => targetMetadataGeom, targetNumPartition)//.persist(StorageLevel.DISK_ONLY)
    val SOIL_AWC_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(SOIL_AWC_RDD, RasterMetadata => targetMetadataGeom, targetNumPartition)//.persist(StorageLevel.DISK_ONLY)
    val SOIL_FC_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(SOIL_FC_RDD, RasterMetadata => targetMetadataGeom, targetNumPartition)//.persist(StorageLevel.DISK_ONLY)
    val NLCD_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(NLCD_RDD_Float, RasterMetadata => targetMetadataGeom, targetNumPartition)//.persist(StorageLevel.DISK_ONLY)
    val ELEV_reshape_part = enforcePartitionerAndSort2(ELEV_reshape, part).persist(StorageLevel.DISK_ONLY)//.mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
    val SOIL_AWC_reshape_part = enforcePartitionerAndSort2(SOIL_AWC_reshape, part).persist(StorageLevel.DISK_ONLY)
    val SOIL_FC_reshape_part = enforcePartitionerAndSort2(SOIL_FC_reshape,  part).persist(StorageLevel.DISK_ONLY)
    val NLCD_reshape_part = enforcePartitionerAndSort2(NLCD_reshape, part).persist(StorageLevel.DISK_ONLY)
    ELEV_reshape_part.count()
    SOIL_AWC_reshape_part.count()
    SOIL_FC_reshape_part.count()
    NLCD_reshape_part.count()

    val NLDAS = "NLDAS_2023_Geotiff"
    val NLDAS_filter = listDateFolders(new Path(NLDAS), FileSystem.get(sc.hadoopConfiguration))
    var previousCycle: RDD[(Int,ITile[Array[Float]])] = null
    var count = 0
    while (!currentDate.isAfter(endDate)) {
      val dateStr = currentDate.toString // e.g., "2023-01-01"
      val targetMonthDay = extractMonthDay(currentDate) // Extract "MM-dd" from current date
      val startTimestamp = getDateTimestamp(dateStr)
      println(s"Constructed datetime string: ", startTimestamp)

      val maybeFolderPrism: Option[String] = PRISM_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      val PRISM_RDD: RasterRDD[Float] = sc.geoTiff(maybeFolderPrism.get)
      val PRISM_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(PRISM_RDD, RasterMetadata => targetMetadataGeom, targetNumPartition) //.persist(StorageLevel.DISK_ONLY)
      val PRISM_reshape_part = enforcePartitioner2(PRISM_reshape)
      val single_band_overlay: RDD[(Int, ITile[Array[Float]])] = RasterOperationsLocal.overlayArrayStreamsF(SOIL_AWC_reshape_part, SOIL_FC_reshape_part, NLCD_reshape_part, ELEV_reshape_part, PRISM_reshape_part).persist(StorageLevel.DISK_ONLY) //.asInstanceOf[RasterRDD[Array[Float]]]
      single_band_overlay.foreachPartition(_ => ())

      val matchingLandsatFolders_B4: Map[LocalDate, Seq[String]] = new LoadLandsat().findLandsatFilesForTwoMonthWindowPaths(currentDate_landsat, Landsat_daily_B4, 10, sc.hadoopConfiguration)
      val filesMap_B4: Array[String] = matchingLandsatFolders_B4.toSeq
        .flatMap { case (_, paths) => paths }
        .toArray
      // Combine all file paths into one whole string separated by spaces.
      val wholePathStringB4: String = filesMap_B4.mkString(" ")
      println(wholePathStringB4)
      println(s"Matching Landsat B4 folders: $wholePathStringB4")

      val matchingLandsatFolders_B5: Map[LocalDate, Seq[String]] = new LoadLandsat().findLandsatFilesForTwoMonthWindowPaths(currentDate_landsat, Landsat_daily_B5, 10, sc.hadoopConfiguration)
      // Flatten the Map: sort by date (if desired), then extract just the file paths.
      val filesMap_B5: Array[String] = matchingLandsatFolders_B5.toSeq
        .flatMap { case (_, paths) => paths }
        .toArray
      // Combine all file paths into one whole string separated by spaces.
      val wholePathStringB5: String = filesMap_B5.mkString(" ")
      println(wholePathStringB5)

      val Landsat_RDD_B4: RasterRDD[Int] = sc.geoTiff(wholePathStringB4)
      val Landsat_RDD_B5: RasterRDD[Int] = sc.geoTiff(wholePathStringB5)
      val landsat_RDD_B4_reshape: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(Landsat_RDD_B4, RasterMetadata => targetMetadataGeom)
      val landsat_RDD_B5_reshape: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(Landsat_RDD_B5, RasterMetadata => targetMetadataGeom)
      val LandsatOverlay: RasterRDD[Array[Int]] = RasterOperationsLocal.overlay(landsat_RDD_B4_reshape, landsat_RDD_B5_reshape)
      val Landsat: RasterRDD[Array[Float]] = RasterOperationsLocal.mapPixels(LandsatOverlay, (p: Array[Int]) => {
        val value = ((p(1) - p(0)).toFloat / (p(1) + p(0)).toFloat).toFloat
        var pixelValue = value
        if (value > 1) {
          pixelValue = 1
        } else if (value < -1) {
          pixelValue = -1
        }
        val LAI = new CalculationLAI().ndviLaiFunc(value)
        Array(pixelValue, LAI.toFloat)
      })
      val landsatP = enforcePartitionerAndSort(Landsat, part)
      landsatP.persist(StorageLevel.DISK_ONLY)
      //Landsat.count()
      landsatP.foreachPartition(_ => ()) // Determine partition count & create partitioner


      // after you build `single_band_overlay` and `Landsat` for the day:
      //      single_band_overlay.persist(StorageLevel.DISK_ONLY)
      //      single_band_overlay.foreachPartition(_ => ()) // Determine partition count & create partitioner
      var hour = 0
      val checkpointInterval = 6 // every 6 hours
      //val sortedPathsIterator = sortedPaths.iterator
      val maybeFolderNLDAS: Option[String] = NLDAS_filter
        .find { case (monthDay, _) => monthDay == targetMonthDay }
        .map(_._2)

      val NLDAS_file = maybeFolderNLDAS.getOrElse {
        throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
      }
      val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray
      // Parallel reshape NLDAS hourly images
      val reshapedBuffer = new ConcurrentLinkedQueue[(String, RDD[(Int, ITile[Array[Float]])])]()
      Parallel2.forEach(NLDAS_RasterRDD.length, (startIdx, endIdx) => {
        for (i <- startIdx until endIdx) {
          val (timestamp, path) = NLDAS_RasterRDD(i)
          val nldasDate = timestamp.toLocalDateTime.toLocalDate
          val rawIn: RasterRDD[Array[Float]] = sc.geoTiff(path)

          val raw = rawIn.mapPixels(p => Array(
            p(0), // Air Temp
            p(1), // Surface Humidity
            Math.sqrt(p(3) * p(3) + p(4) * p(4)).toFloat, // Wind Speed
            p(5) // Shortwave
          ))

          val reshaped: RasterRDD[Array[Float]] = RasterOperationsFocal.reshapeNN(raw, _ => targetMetadataGeom, targetNumPartition)
          val outputRDD = reshaped.mapPixels { p =>
            Array(
              if (p.length > 0) p(0) else -9999f,
              if (p.length > 1) p(1) else -9999f,
              if (p.length > 2) p(2) else -9999f,
              if (p.length > 3) p(3) else -9999f
            )
          }
          val repart: RDD[(Int, ITile[Array[Float]])] = enforcePartitionerAndSort(outputRDD, part)
          val nldasName = new Path(path.toString).getName
          repart.persist(StorageLevel.DISK_ONLY)
          repart.count() // Trigger computation and persist // took 1077.267087 s
          reshapedBuffer.add((nldasName, repart))
        }
      })

      val hourPattern = """.*\.(\d{4})\.\d{3}\.tif$""".r
      val reshapedList = reshapedBuffer.asScala.toList
        .sortBy { case (name, _) =>
          name match {
            case hourPattern(hourStr) => hourStr.toInt
            case _ =>
              println(s"[WARN] Could not extract hour from filename: $name")
              0 // Default to 0 if the pattern doesn't match
          }
        }

      // Where we’ll write/read the “previous hour” state
      val basePath = "/tmp/previousCycle"
      val lastCycleDir = s"$basePath/lastCycle"
      // At the top of this iteration, if previousCycle is null, try loading it
      if (previousCycle == null) {
        val lastCyclePath = s"$basePath/lastCycle-$startDate-$endDate"
        val p = new Path(lastCyclePath)
        if (fs.exists(p)) {
          previousCycle = sc
            .objectFile[(Int, ITile[Array[Float]])](lastCyclePath, targetNumPartition)
            //.partitionBy(part)
        }
        // otherwise it remains null for the first pass
      }
      reshapedList.foreach { case (name, nldasRdd) => {
        println(s"[INFO] Processing hour: $name")
        // build the overlay stream, chaining previousCycle if it exists
        val layerTiles =
          if (previousCycle == null)
            RasterOperationsLocal.overlayArrayStreamsF2(single_band_overlay, nldasRdd, landsatP)
          else
            RasterOperationsLocal.overlayArrayStreamsF2(single_band_overlay, nldasRdd, landsatP, previousCycle)
        // your domain-specific per-tile computation
        val newCycle = BandCalculationCheck2.IterativeCalculationPre2(layerTiles, null)
        // Unpersist prior RDD (if any) and persist the new one
        if (previousCycle != null) previousCycle.unpersist(false)
        newCycle.persist()
        // Overwrite previousCycle for the next hour
        previousCycle = newCycle
        // After all hours for this date range, save out `previousCycle` for the next date
        if (previousCycle != null) {
          val lastCyclePath = s"$basePath/lastCycle-$startDate-$endDate"
          fs.delete(new Path(lastCyclePath), true)
          previousCycle
            .map { case (k, tile) => (k, tile) }
            .saveAsObjectFile(lastCyclePath)
        }
        // advance
        hour += 1
        println(s"[INFO] Finished Processing hour: $name")
        println(s"✅ Finished hour $hour")
        val time = (System.nanoTime() - startTime) / 1E9
        println(s"📐 So far running time checkpoint: $time}")
      }
        Landsat.unpersist()
        landsatP.unpersist()
        single_band_overlay.unpersist()
        // Increment to the next day.
        currentDate = currentDate.plusDays(1)
        currentDate_landsat = currentDate_landsat.plusDays(1)
        count += 1
        // after the for (filename <- sortedPaths) { … } loop, before moving to next date:
      }
    }
      val output_file = s"$output/landsat-$startDate-$endDate.nn.tif"
      val filter = previousCycle.values.filter(_ != null)
      val outputRDD = filter.mapPixels(p => { // allocate exactly 5 floats, pre-fill with -9999f
        val out = Array.fill(5)(-9999f)
        // copy at most min(p.length, 5) values
        val n = math.min(p.length, 5)
        var i = 0
        while (i < n) {
          out(i) = p(i)
          i += 1
        }
        out
      })
      GeoTiffWriter.saveAsGeoTiff(outputRDD, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.OutputNumBands -> "5", GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
      val endTime = System.nanoTime()
      println(s"Total stages executed: ${listener.stageCount}")
      println("Total time of whole process : Reshape rasterRDD: " + (endTime - startTime) / 1E9)
      spark.stop()
  }
}
