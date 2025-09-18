package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel_listFile.{AOI_metadata, Raster_metadata, extractMonthDay, getClass, getDateTimestamp, listDateFolders}
import com.baitsss.model.PipelineLandsatETModel_listFile_test.extractMonthDay
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.util.Parallel2
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
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object PipelineLandsatETModel_listFile_testp {
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

    val geom = Reprojector.reprojectGeometry(AOI.first().getGeometry, targetMetadata._1.srid) // calling long time
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
  def extractMonthDay(date: LocalDate): String =
    date.format(DateTimeFormatter.ofPattern("MM-dd"))

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val conf = new SparkConf().setAppName("TestReshapeNN").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    conf.set("fs.defaultFS", "file:///")

    val startTotalTime = System.nanoTime()

    // --- INPUTS ---
    // AOI
    val AOI_shp = "file:///home/zshan011/CA/CA.shp"
    val AOI: RDD[IFeature] = sc.shapefile(AOI_shp)
    val targetCRS = CRS.decode("EPSG:4326") //4326, 3857

    val NLDAS = "file:///home/zshan011/NLDAS_2023_Geotiff"
    val Landsat_daily_B4 = "file:///home/zshan011/California_Landsat8_4B_2021/B4"
    val Landsat_daily_B5 = "file:///home/zshan011/California_Landsat8_4B_2021/B5"

    // Define start and end dates.
    val startDate = LocalDate.parse("2023-01-01")
    val endDate = LocalDate.parse("2023-01-01")
    val startDate_landsat = LocalDate.parse("2021-01-01")
    val endDate_landsat = LocalDate.parse("2021-01-07")
    var currentDate = startDate
    var currentDate_landsat = startDate_landsat
    // Define your desired window: if NLDAS key indicates day "01", then window is day 1 to day 16.
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val landsatDate = startDate_landsat
    val matchingLandsatFolders_B4: Map[LocalDate, Seq[String]] = new LoadLandsat().findLandsatFilesForTwoMonthWindowPaths(landsatDate, Landsat_daily_B4, 10, sc.hadoopConfiguration)
    val filesMap_B4: Array[String] = matchingLandsatFolders_B4.toSeq
      .flatMap { case (_, paths) => paths }
      .toArray
    // Combine all file paths into one whole string separated by spaces.
    val wholePathStringB4: String = filesMap_B4.mkString(" ")
    val Landsat_RDD_B4: RasterRDD[Int] = sc.geoTiff(wholePathStringB4)
    val Landsat_metadata = Landsat_RDD_B4.first().rasterMetadata
    val Landsat_all_metadata = RasterMetadata.allMetadata(Landsat_RDD_B4)
    val Landsat_RDD_Metadata = Raster_metadata(Landsat_metadata, Landsat_all_metadata, targetCRS)
    val targetMetadataGeom = AOI_metadata(AOI, Landsat_RDD_Metadata)

    // --- Initialize empty buffer ---
    val allNLDAS: scala.collection.mutable.ArrayBuffer[(Timestamp, String)] = scala.collection.mutable.ArrayBuffer()
    val NLDAS_filter = PipelineLandsatETModel_listFile_testp.listDateFolders(new Path(NLDAS), FileSystem.get(sc.hadoopConfiguration))

    while (!currentDate.isAfter(endDate)) {
      val todayStr = currentDate.toString // e.g., "2023-01-01"
      val targetMonthDay = extractMonthDay(currentDate) // "01-01"

      // Find matching NLDAS folder
      val maybeFolderNLDAS: Option[String] = NLDAS_filter.find { case (monthDay, _) => monthDay == targetMonthDay }.map(_._2)

      if (maybeFolderNLDAS.isDefined) {
        val NLDAS_folder = maybeFolderNLDAS.get
        println(s"[INFO] Found NLDAS folder for $todayStr: $NLDAS_folder")

        // List hourly .tif files for this folder
        val todayNLDAS: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_folder).toArray

        // Add to big buffer
        allNLDAS ++= todayNLDAS
      } else {
        println(s"[WARN] No NLDAS folder found for $todayStr, skipping.")
      }

      currentDate = currentDate.plusDays(1)
    }
    // Convert to array
    val NLDAS_RasterRDD: Array[(Timestamp, String)] = allNLDAS.toArray.slice(0,5)

    println(s"[INFO] Total NLDAS hourly files collected: ${NLDAS_RasterRDD.length}")
    val output = "file:///home/zshan011/aoidata_nldas_all_reshape"
    val outputPath = new Path(output)
    if (!fs.exists(outputPath)) {
      fs.mkdirs(outputPath)
    }
    // Parallel reshape NLDAS hourly images
     Parallel2.forEach(NLDAS_RasterRDD.length, (startIdx, endIdx) => {
        for (i <- startIdx until endIdx)  {
          val startTotalTime = System.nanoTime()
          val (timestamp, path) = NLDAS_RasterRDD(i)
          // Check if Landsat needs reload
          val nldasDate = timestamp.toLocalDateTime.toLocalDate

          println(s"[INFO] Reshaping NLDAS file: $path")

          // Read file
          val rawIn: RasterRDD[Array[Float]] = sc.geoTiff(path)

          // Preprocess bands (optional, like wind speed calculation)
          val raw = rawIn.mapPixels(p => Array(
            p(0), // Air Temp
            p(1), // Surface Humidity
            Math.sqrt(p(3) * p(3) + p(4) * p(4)).toFloat, // Wind Speed
            p(5) // Shortwave
          ))

          // Reshape to target geometry
          val reshaped: RasterRDD[Array[Float]] = RasterOperationsFocal.reshapeNN(raw, _ => targetMetadataGeom)
          val outputRDD = reshaped.mapPixels { p =>
            Array(
              if (p.length > 0) p(0) else -9999f,
              if (p.length > 1) p(1) else -9999f,
              if (p.length > 2) p(2) else -9999f,
              if (p.length > 3) p(3) else -9999f,
            )
          }

          val nldas_str = nldasDate.toString
          val nldas_name = new Path(path.toString).getName
          val output_file = s"$output/$nldas_str/$nldas_name.lzw.tif"
          val dateFolder = s"$output/$nldas_str"
          val dateFolderPath = new Path(dateFolder)
          if (!fs.exists(dateFolderPath)) {
            fs.mkdirs(dateFolderPath)
          }
          GeoTiffWriter.saveAsGeoTiff(outputRDD, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.OutputNumBands->"4",GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
          println(s"[INFO] Finish Reshaping NLDAS file: $nldas_name")
          val t2 = System.nanoTime()
          val totalElapsed = (t2 - startTotalTime) / 1e9
          println(s"[INFO] Finish Reshaping NLDAS file: $nldas_name with time: $totalElapsed%.2f seconds")
        }
      }) // 👈 You can adjust 8 or 16
    // Flatten results across threads

    val totalElapsed = (System.nanoTime() - startTotalTime) / 1e9

    println(f"------ TOTAL reshapeNN TIME (parallel across 24 hours): $totalElapsed%.2f seconds ------")

    spark.stop()
  }
}
