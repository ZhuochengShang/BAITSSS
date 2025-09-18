package com.baitsss.model

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal, StackedTile}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
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

object PipelinePlanetETModel_test {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def sortTilesByID[T](rdd: RasterRDD[T]): RasterRDD[T] = {
    rdd.map(t => (t.tileID, t)) // (Int, ITile)
      .mapPartitions(iter => iter.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
      .map(_._2)
      .asInstanceOf[RasterRDD[T]]
  }

  // Helper to print an RDD's partitioner
  def showPart(name: String, rdd: RasterRDD[_]): Unit = {
    val desc = rdd.partitioner match {
      case Some(p) => s"${p.getClass.getSimpleName}(partitions=${p.numPartitions})"
      case None => "None"
    }
    log.info(s"Partitioner of $name: $desc")
  }

  /**
   * Throws IllegalArgumentException if any of the given RasterRDDs
   * has a different Partitioner (or, if none have explicit partitioners,
   * a different number of partitions).
   */
  def assertSamePartitioning(rdds: (String, RasterRDD[_])*): Unit = {
    // Gather each RDD’s partitioner and partition count
    val info = rdds.map { case (name, r) =>
      (name,
        r.partitioner, // Option[Partitioner]
        r.getNumPartitions // Int
      )
    }
    // Check that all Option[Partitioner] values are identical
    val distinctParts = info.map(_._2).distinct
    require(distinctParts.size == 1,
      "Partitioner mismatch:\n" +
        info.map { case (n, p, _) => s"  $n.partitioner = $p" }.mkString("\n")
    )
    // If they all have no explicit Partitioner, check the counts match
    if (distinctParts.head.isEmpty) {
      val distinctCounts = info.map(_._3).distinct
      require(distinctCounts.size == 1,
        "NumPartitions mismatch (no explicit Partitioner):\n" +
          info.map { case (n, _, c) => s"  $n.partitions = $c" }.mkString("\n")
      )
    }
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
          println("planet: ",timestamp)
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
    val conf = new SparkConf().setAppName("OptimizedMultiTemporalAggregator")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setCheckpointDir("/user/zshan011/checkpoints")
    val listener = new StageCountListener()
    sc.addSparkListener(listener)

    val startTime = System.nanoTime()

    val output = "aoidata_rdpro_planet_la"
    val AOI_shp = "LA/POLYGON.shp"
    val AOI = sc.shapefile(AOI_shp)
    val targetCRS = CRS.decode("EPSG:4326")

    val elevation_yearly = "NLDAS_Elevation_2023_Geotiff"
    val soil_yearly_awc = "Soil_2023/awc_gNATSGO_US.tif"
    val soil_yearly_fc = "Soil_2023/fc_gNATSGO_US.tif"
    val nlcd_yearly = "NLCD_2023"

    val prism_daily = "PRISM_2023_Geotiff"
    val Planet_daily = "Planet_CA_4B_BAITSSS"
    val NLDAS = "NLDAS_2023_Geotiff"

    val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration))
    val Planet_filter = listDateFolders(new Path(Planet_daily), FileSystem.get(sc.hadoopConfiguration))
    val NLDAS_filter = listDateFolders(new Path(NLDAS), FileSystem.get(sc.hadoopConfiguration))

    val startDate = LocalDate.parse("2023-01-01")
    val endDate = LocalDate.parse("2023-01-01")
    var currentDate = startDate
    var previousCycle: RasterRDD[Array[Float]] = null

    val ELEV_RDD: RasterRDD[Float] = sc.geoTiff(elevation_yearly)
    val SOIL_AWC_RDD: RasterRDD[Float] = sc.geoTiff(soil_yearly_awc) //.persist(StorageLevel.DISK_ONLY)
    val SOIL_FC_RDD: RasterRDD[Float] = sc.geoTiff(soil_yearly_fc) //.persist(StorageLevel.DISK_ONLY)
    val NLCD_RDD: RasterRDD[Int] = sc.geoTiff(nlcd_yearly)
    val NLCD_RDD_Float = NLCD_RDD.mapPixels(_.toFloat) //.persist(StorageLevel.DISK_ONLY)

    while (!currentDate.isAfter(endDate)) {
      val targetMonthDay = extractMonthDay(currentDate)
      val dateStr = currentDate.toString

      val planetFolder = Planet_filter.find(_._1 == targetMonthDay).map(_._2).get
      val Planet_RDD: RasterRDD[Array[Int]] = sc.geoTiff(planetFolder)
      val PlanetFloat = Planet_RDD.mapPixels { p =>
        val ndvi = ((p(1) - p(0)).toFloat / (p(1) + p(0)).toFloat).max(-1f).min(1f)
        val LAI = new CalculationLAI().ndviLaiFunc(ndvi)
        Array(ndvi, LAI.toFloat)
      }
      val Planet_meta = PlanetFloat.first().rasterMetadata
      val Planet_RDD_Metadata = Raster_metadata(Planet_meta, RasterMetadata.allMetadata(PlanetFloat), targetCRS)
      val targetMetadataGeom = AOI_metadata(AOI, Planet_RDD_Metadata)

      val Planet_reshaped = RasterOperationsFocal.reshapeNN(PlanetFloat, _ => targetMetadataGeom)
      val part = new HashPartitioner(Planet_reshaped.getNumPartitions)
      val planetAligned = Planet_reshaped.map(t => (t.tileID, t)).partitionBy(part).map(_._2).persist(StorageLevel.DISK_ONLY)
      planetAligned.foreachPartition(_ => ())

      def staticLayer(rdd: RasterRDD[Float]) =
        RasterOperationsFocal.reshapeNN(rdd, _ => targetMetadataGeom, part.numPartitions)
          .map(t => (t.tileID, t)).partitionBy(part).map(_._2)

      val ELEV: RasterRDD[Float] = staticLayer(ELEV_RDD).persist(StorageLevel.DISK_ONLY)
      val SOIL_AWC: RasterRDD[Float] = staticLayer(SOIL_AWC_RDD).persist(StorageLevel.DISK_ONLY)
      val SOIL_FC: RasterRDD[Float] = staticLayer(SOIL_FC_RDD).persist(StorageLevel.DISK_ONLY)
      val NLCD: RasterRDD[Float] = staticLayer(NLCD_RDD_Float).persist(StorageLevel.DISK_ONLY)

      val PRISM_path = PRISM_filter.find(_._1 == targetMonthDay).map(_._2).get
      val PRISM_RDD: RasterRDD[Float] = sc.geoTiff(PRISM_path)
      val PRISM: RasterRDD[Float] = staticLayer(PRISM_RDD).persist(StorageLevel.DISK_ONLY)

      val staticOverlay: RasterRDD[Array[Float]] =
        RasterOperationsLocal.overlay(SOIL_AWC, SOIL_FC, NLCD, ELEV, PRISM)
          .persist(StorageLevel.DISK_ONLY)

      staticOverlay.foreachPartition(_ => ()) // Trigger materialization

      val NLDAS_path = NLDAS_filter.find(_._1 == targetMonthDay).map(_._2).get
      val NLDAS_files = FileReader.ListDate_NLDAS_hourly(NLDAS_path).toArray

      for ((_, file) <- NLDAS_files.zipWithIndex) {
        val rawIn: RasterRDD[Array[Float]] = sc.geoTiff(NLDAS_files(file)._2)
        val raw = rawIn.mapPixels(p => Array(p(0), p(1), Math.sqrt(p(3) * p(3) + p(4) * p(4)).toFloat, p(5)))
        val reshaped = RasterOperationsFocal.reshapeNN(raw, _ => targetMetadataGeom, part.numPartitions)
          .map(t => (t.tileID, t)).partitionBy(part).map(_._2)

//        val layer3 = staticOverlay.map(t => (t.tileID, t))
//          .join(reshaped.map(t => (t.tileID, t)), part)
//          .join(planetAligned.map(t => (t.tileID, t)), part)
//          .join((if (previousCycle != null) previousCycle else staticOverlay).map(t => (t.tileID, t)), part)
//          .mapValues { case (((s, u), p), prev) =>
//            new StackedTile[Float](Array(s, u, p) ++ Option(prev).toArray: _*)
//          }.values.asInstanceOf[RasterRDD[Array[Float]]]

        val result = BandCalculationCheck2.IterativeCalculationPre(reshaped, null).persist(StorageLevel.DISK_ONLY)
        if (previousCycle != null) previousCycle.unpersist()
        previousCycle = result
      }

      currentDate = currentDate.plusDays(1)
    }

    val outputPath = s"$output/planet-$startDate-$endDate.tif"
    GeoTiffWriter.saveAsGeoTiff(previousCycle, outputPath, Seq(
      GeoTiffWriter.WriteMode -> "compatibility",
      GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_NONE,
      GeoTiffWriter.BigTiff -> "yes",
      GeoTiffWriter.FillValue -> "-9999"
    ))

    val endTime = System.nanoTime()
    println(s"Total time: ${(endTime - startTime) / 1E9}s")
    spark.stop()
  }
}
