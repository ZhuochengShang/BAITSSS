package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel_listFile.{getClass, getDateTimestamp}
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
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
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object PipelineLandsatETModel_readwrite {
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
  def extractMonthDay(date: LocalDate): String =
    date.format(DateTimeFormatter.ofPattern("MM-dd"))

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val conf = new SparkConf().setAppName("TestReshapeNN").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val startTotalTime = System.nanoTime()

    // --- INPUTS ---
    // AOI
    val AOI_shp = "CA/CA.shp"
    val AOI: RDD[IFeature] = sc.shapefile(AOI_shp)
    val targetCRS = CRS.decode("EPSG:4326") //4326, 3857


    val Landsat_daily_B4 = "California_Landsat8_4B_2021/B4"
    val Landsat_daily_B5 = "California_Landsat8_4B_2021/B5"

    // Define start and end dates.
    val startDate = LocalDate.parse("2023-01-01")
    val endDate = LocalDate.parse("2023-01-01")
    val startDate_landsat = LocalDate.parse("2021-01-01")
    val endDate_landsat = LocalDate.parse("2021-01-01")
    var currentDate_landsat = startDate_landsat

    val dateStr = startDate.toString // e.g., "2023-01-01"
    val targetMonthDay = extractMonthDay(startDate_landsat) // Extract "MM-dd" from current date
    val startTimestamp = getDateTimestamp(dateStr)
    println(s"Constructed datetime string: ", startTimestamp)

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
      // --- INPUTS ---
      val NLDAS_LZW: RasterRDD[Array[Float]] = sc.geoTiff("aoidata_nldas/nldas-2023-01-01.0.lzw.tif")
      val NLDAS_NON: RasterRDD[Array[Float]] = sc.geoTiff("aoidata_nldas/nldas-2023-01-01.0.non.tif")

    val NLDAS_LZW_overlay  = RasterOperationsLocal.overlay(NLDAS_LZW, Landsat)
    val NLDAS_NON_overlay  = RasterOperationsLocal.overlay(NLDAS_NON, Landsat)
    val outputRDD = NLDAS_LZW_overlay.mapPixels { p =>
      Array(
        if (p.length > 0) p(0) else -9999f,
        if (p.length > 1) p(1) else -9999f,
        if (p.length > 2) p(2) else -9999f,
        if (p.length > 3) p(3) else -9999f,
        if (p.length > 4) p(4) else -9999f
      )
    }

    val output = "aoidata_nldas_ca"
    val output_file = s"$output/nldas-2023-01-01.lzw.tif"
    val output_file2 = s"$output/nldas-2023-01-01.non.tif"
    GeoTiffWriter.saveAsGeoTiff(outputRDD, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW, GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    //GeoTiffWriter.saveAsGeoTiff(outputRDD, output_file2, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_NONE, GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"


    // --- Total Time ---
      val totalElapsed = (System.nanoTime() - startTotalTime) / 1e9
      println(f"------ TOTAL reshapeNN TIME: $totalElapsed%.2f seconds ------")

      spark.stop()
  }
}
