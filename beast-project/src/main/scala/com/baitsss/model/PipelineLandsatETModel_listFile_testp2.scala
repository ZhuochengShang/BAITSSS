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

object PipelineLandsatETModel_listFile_testp2 {
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

    val startTotalTime = System.nanoTime()

    // --- INPUTS ---
    // AOI
    val AOI_shp = "CA/CA.shp"
    val AOI: RDD[IFeature] = sc.shapefile(AOI_shp)
    val targetCRS = CRS.decode("EPSG:4326") //4326, 3857

    val elevation_yearly = "NLDAS_Elevation_2023_Geotiff"
    val soil_yearly_awc = "Soil_2023/awc_gNATSGO_US.tif"
    val soil_yearly_fc = "Soil_2023/fc_gNATSGO_US.tif"
    val nlcd_yearly = "NLCD_2023"
    val prism_daily = "PRISM_2023_Geotiff"
    val Landsat_daily_B4 = "California_Landsat8_4B_2021/B4"

    val output = "single_band_all"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if (!fs.exists(new Path(output))) fs.mkdirs(new Path(output))

    // --- Preload Static Layers ---
    val ELEV_RDD: RasterRDD[Float] = sc.geoTiff(elevation_yearly)//.persist(StorageLevel.DISK_ONLY)
    val SOIL_AWC_RDD: RasterRDD[Float] = sc.geoTiff(soil_yearly_awc)//.persist(StorageLevel.DISK_ONLY)
    val SOIL_FC_RDD: RasterRDD[Float] = sc.geoTiff(soil_yearly_fc)//.persist(StorageLevel.DISK_ONLY)
    val NLCD_RDD: RasterRDD[Int] = sc.geoTiff(nlcd_yearly)
    val NLCD_RDD_Float: RasterRDD[Float] = NLCD_RDD.mapPixels(_.toFloat)//.persist(StorageLevel.DISK_ONLY)

    // --- Date Ranges ---
    val startDate = LocalDate.parse("2023-01-05")
    val endDate = LocalDate.parse("2023-01-07")

    // --- List PRISM Daily ---
    val PRISM_filter = listDateFolders(new Path(prism_daily), fs)

    val allDates = Iterator.iterate(startDate)(_.plusDays(1))
      .takeWhile(!_.isAfter(endDate))
      .toArray

//    Parallel2.forEach(0, allDates.length, (startIdx, endIdx) => {
//      for (i <- startIdx until endIdx) {
        val currentDate = allDates(1)

        val todayStr = currentDate.toString
        val targetMonthDay = extractMonthDay(currentDate)

        val maybeFolderPRISM = PRISM_filter.find { case (monthDay, _) => monthDay == targetMonthDay }.map(_._2)
        if (maybeFolderPRISM.isEmpty) {
          println(s"[WARN] No PRISM found for $todayStr")
        } else {
          val prismPath = maybeFolderPRISM.get
          val landsatDate = LocalDate.of(2021, currentDate.getMonthValue, currentDate.getDayOfMonth)
          val matchingLandsatFolders_B4 = new LoadLandsat().findLandsatFilesForTwoMonthWindowPaths(landsatDate, Landsat_daily_B4, 10, sc.hadoopConfiguration)
          val filesMap_B4 = matchingLandsatFolders_B4.toSeq.flatMap { case (_, paths) => paths }.toArray
          val wholePathStringB4 = filesMap_B4.mkString(" ")

          val Landsat_RDD_B4: RasterRDD[Array[Int]] = sc.geoTiff(wholePathStringB4)
          val Landsat_metadata = Landsat_RDD_B4.first().rasterMetadata
          val Landsat_all_metadata = RasterMetadata.allMetadata(Landsat_RDD_B4)
          val Landsat_RDD_Metadata = Raster_metadata(Landsat_metadata, Landsat_all_metadata, targetCRS)
          val targetMetadataGeom = AOI_metadata(AOI, Landsat_RDD_Metadata)

          val targetNumPartition = Landsat_RDD_B4.getNumPartitions

          val ELEV_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(ELEV_RDD, _ => targetMetadataGeom, targetNumPartition)
          val SOIL_AWC_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(SOIL_AWC_RDD, _ => targetMetadataGeom, targetNumPartition)
          val SOIL_FC_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(SOIL_FC_RDD, _ => targetMetadataGeom, targetNumPartition)
          val NLCD_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(NLCD_RDD_Float, _ => targetMetadataGeom, targetNumPartition)
          val PRISM_RDD: RasterRDD[Float] = sc.geoTiff(prismPath)
          val PRISM_reshape: RasterRDD[Float] = RasterOperationsFocal.reshapeNN(PRISM_RDD, _ => targetMetadataGeom, targetNumPartition)

          val staticOverlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(SOIL_AWC_reshape, SOIL_FC_reshape, NLCD_reshape, ELEV_reshape, PRISM_reshape)
          val outputRDD = staticOverlay.mapPixels { p =>
            Array(
              if (p.length > 0) p(0) else -9999f,
              if (p.length > 1) p(1) else -9999f,
              if (p.length > 2) p(2) else -9999f,
              if (p.length > 3) p(3) else -9999f,
              if (p.length > 4) p(4) else -9999f,
            )
          }
          val dateFolder = s"$output/$todayStr"
          if (!fs.exists(new Path(dateFolder))) fs.mkdirs(new Path(dateFolder))
          val output_file = s"$dateFolder/single_band_overlay.lzw.tif"

          GeoTiffWriter.saveAsGeoTiff(outputRDD, output_file, Seq(
            GeoTiffWriter.WriteMode -> "compatibility",
            GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW,
            GeoTiffWriter.BigTiff -> "yes",
            GeoTiffWriter.FillValue -> "-9999"
          ))

          println(s"[INFO] Finished processing day $todayStr ✅")
        }
//      }
//    })


    val totalElapsed = (System.nanoTime() - startTotalTime) / 1e9

    println(f"------ TOTAL reshapeNN TIME (parallel across 24 hours): $totalElapsed%.2f seconds ------")

    spark.stop()
  }
}
