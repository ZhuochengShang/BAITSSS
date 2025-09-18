package com.baitsss.model

import com.baitsss.model.PipelineLandsatETModel_listFile_all.listDateFolders
import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants
import edu.ucr.cs.bdlab.beast.util.Parallel2
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.commons.math3.util.FastMath.hypot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
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

object PipelineLandsatETModel {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  private val dateFmt            = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val mdFmt              = DateTimeFormatter.ofPattern("MM-dd")
  private val checkpointInterval = 4 // checkpoint every 4 hours to truncate lineage
  private val starttime = System.nanoTime()

  // --- Helpers --------------------------------------------------------------

  def extractMonthDay(ts: Timestamp): String = ts.toLocalDateTime.format(mdFmt)
  def extractMonthDay(date: LocalDate): String = date.format(mdFmt)

  def toDummyTimestamp(dateStr: String): Timestamp = {
    val parts = dateStr.split("-")
    val dt = LocalDateTime.parse(
      s"1970 ${parts(1)} ${parts(2)} 0000",
      DateTimeFormatter.ofPattern("yyyy MM dd HHmm")
    )
    Timestamp.valueOf(dt)
  }

  def listDateFolders(base: Path, fs: FileSystem): Array[(String, String)] = {
    val logger = LoggerFactory.getLogger(getClass)
    val dateRx = "\\d{4}-\\d{2}-\\d{2}".r
    if (!fs.exists(base) || !fs.getFileStatus(base).isDirectory) return Array.empty
    fs.listStatus(base).flatMap { st =>
      if (st.isDirectory) {
        val name = st.getPath.getName
        dateRx.findFirstIn(name).map { _ =>
          try {
            val d   = LocalDate.parse(name, dateFmt)
            val key = d.format(mdFmt)
            key -> st.getPath.toString
          } catch {
            case e: Exception => logger.warn(s"Skipping invalid folder $name", e); null
          }
        }
      } else None
    }.filter(_ != null)
  }

  def findLandsatFilesForTwoMonthWindowPaths(
                                              targetDate: LocalDate,
                                              baseFolder: String,
                                              thresholdDays: Int,
                                              hadoopConf: Configuration
                                            ): Map[LocalDate, Seq[String]] = {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val startDate     = targetDate.minusWeeks(2)
    val endDate       = targetDate.plusWeeks(2)
    val fs            = FileSystem.get(hadoopConf)
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

  def processDay(
                  sc: SparkContext,
                  date: LocalDate,
                  landsatDate: LocalDate,
                  aoi: RDD[IFeature],
                  metadata: RasterMetadata,
                  fs: FileSystem,
                  numParts: Int,
                  part: Partitioner,
                  prevOpt: Option[RDD[(Int, ITile[Array[Float]])]],
                  soilAWCKV: RDD[(Int, ITile[Float])],
                  soilFCKV:  RDD[(Int, ITile[Float])],
                  nlcdKV:    RDD[(Int, ITile[Float])],
                  elevKV:    RDD[(Int, ITile[Float])]
                ): RDD[(Int, ITile[Array[Float]])] = {
    // PRISM
    //val prismPath = listDateFolders(new Path("PRISM_2023_Geotiff"), fs).find(_._1==extractMonthDay(date)).get._2
    val startDaytime = System.nanoTime()
    val targetMonthDay = extractMonthDay(date)
    val prism_daily = "PRISM_2023_Geotiff" // Contains daily files, e.g., NLDAS_DAILY.AYYYYMMDD.002.tif
    val NLDAS_filter = listDateFolders(new Path("NLDAS_2023_Geotiff"), FileSystem.get(sc.hadoopConfiguration))
    val PRISM_filter = listDateFolders(new Path(prism_daily), FileSystem.get(sc.hadoopConfiguration)) //ListDate_daily_hourly(prism_daily)
    val maybeFolderPrism: Option[String] = PRISM_filter
      .find { case (monthDay, _) => monthDay == targetMonthDay }
      .map(_._2)
    val prismKV   = enforcePartitionKV(
      RasterOperationsFocal.reshapeNN(sc.geoTiff[Float](maybeFolderPrism.get),_=>metadata,numParts), part)

    // Landsat ±1m
    val b4Map   = findLandsatFilesForTwoMonthWindowPaths(landsatDate,"California_Landsat8_4B_2021/B4",10,sc.hadoopConfiguration)
    val b5Map   = findLandsatFilesForTwoMonthWindowPaths(landsatDate,"California_Landsat8_4B_2021/B5",10,sc.hadoopConfiguration)
    val b4RDD: RasterRDD[Int]   = sc.geoTiff[Int](b4Map.values.flatten.mkString(" "))
    val b5RDD: RasterRDD[Int]   = sc.geoTiff[Int](b5Map.values.flatten.mkString(" "))
    val rs4: RasterRDD[Int] = RasterOperationsFocal.reshapeNN(b4RDD, _ => metadata, numParts)
    val rs5: RasterRDD[Int]     = RasterOperationsFocal.reshapeNN(b5RDD,_=>metadata,numParts)
    val lsOv: RasterRDD[Array[Int]]    = RasterOperationsLocal.overlay(rs4, rs5)
    val landsKV = enforcePartitionKV(
      RasterOperationsLocal.mapPixels(lsOv, (p:Array[Int])=>{
        val ndvi=(p(1)-p(0)).toFloat/(p(1)+p(0)).toFloat
        val lai=new CalculationLAI().ndviLaiFunc(ndvi).toFloat
        Array(ndvi,lai)
      }), part, sort=true).persist(StorageLevel.MEMORY_AND_DISK_SER)
    landsKV.foreachPartition(_ => ())

    // Static+PRISM
    val baseKV = RasterOperationsLocal.overlayArrayStreamsF(soilAWCKV, soilFCKV, nlcdKV, elevKV, prismKV)
      .persist(StorageLevel.DISK_ONLY)
    baseKV.foreachPartition(_ => ())

    // Hourly NLDAS
    val maybeFolderNLDAS: Option[String] = NLDAS_filter
      .find { case (monthDay, _) => monthDay == targetMonthDay }
      .map(_._2)

    val NLDAS_file = maybeFolderNLDAS.getOrElse {
      throw new NoSuchElementException(s"No matching NLDAS folder found for date (MM-dd): $targetMonthDay")
    }
    val NLDAS_RasterRDD: Array[(Timestamp, String)] = FileReader.ListDate_NLDAS_hourly(NLDAS_file).toArray
    // replay your "processDay" inner logic for each batch:
      // --- ingest & reshape this batch in parallel
      val buf = new ConcurrentLinkedQueue[(String, RDD[(Int, ITile[Array[Float]])])]()
      Parallel2.forEach(NLDAS_RasterRDD.length, (s,e) => {
        for(i <- s until e) {
          val (_, path) = NLDAS_RasterRDD(i)
          val r    = sc.geoTiff[Array[Float]](path).mapPixels(a => Array(a(0), a(1), hypot(a(3),a(4)).toFloat, a(5)))
          val resh = RasterOperationsFocal.reshapeNN(r, _=>metadata, numParts)
          val kv   = enforcePartitionKV(resh, part, sort=true).persist(StorageLevel.DISK_ONLY)
          kv.foreachPartition(_=>())
          buf.add(path -> kv)
          r.unpersist(false); resh.unpersist(false)
        }
      })

      // now your ordered hours for this batch
      val hours = buf.asScala.toList.sortBy(_._1)
      val ingestT1 = System.nanoTime()
    var cycle: RDD[(Int, ITile[Array[Float]])] = prevOpt.orNull
    // --- rolling overlay just for these 12 hours, seeding with previous cycle if any
      for (((hourName, hrRDD), idx) <- hours.zipWithIndex) {
        val layered   = RasterOperationsLocal.overlayArrayStreamsF2(Seq(baseKV, hrRDD, landsKV) ++ Option(cycle): _*)
        val nextCycle = BandCalculationCheck2.IterativeCalculationPre2(layered, null)

        // force the work to happen
        nextCycle.foreachPartition(_=>())

        // drop this hour
        hrRDD.unpersist(blocking=true)

        // drop prior cycle only after we know its shuffle is done
        if (cycle != null) cycle.unpersist(blocking=true)

        cycle.map(identity).persist(StorageLevel.DISK_ONLY)
        // persist the new
        nextCycle.persist(StorageLevel.DISK_ONLY)
        cycle = nextCycle


        val dayT1 = System.nanoTime()
        println(f"[!!!] Finished hour ${idx+1} in ${(dayT1 - starttime) / 1e9}%.2f s total")
      }

      val tracktime = (System.nanoTime() - startDaytime) / 1e9
      println(f"[!!!] time: $tracktime")

    val dayT1 = System.nanoTime()
    println(f"[!!!] Finished day $date in ${(dayT1 - starttime) / 1e9}%.2f s total")

    // final daily checkpoint
    if (cycle != null) {
      cycle.checkpoint();
      cycle.foreachPartition(_ => ())
      println(s"!!! Completed daily checkpoint for $date")
    }

    landsKV.unpersist(blocking = true)
    baseKV.unpersist(blocking = true)

    cycle
  }
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("MultiTemporalET").setIfMissing("spark.master","local[*]")
    val spark= SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setCheckpointDir("hdfs:///user/zshan011/landsat_ckpt")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // AOI & metadata with two-month sample period
    val aoi = sc.shapefile("CA/CA.shp")
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
    val aoiMeta    = AOI_metadata(aoi, globalMeta)
    val parts      = sample.getNumPartitions * 2
    val part       = new HashPartitioner(parts)

    // Static yearly layers
    val elevRDD   = sc.geoTiff[Float]("NLDAS_Elevation_2023_Geotiff")
    val awcRDD    = sc.geoTiff[Float]("Soil_2023/awc_gNATSGO_US.tif")
    val fcRDD     = sc.geoTiff[Float]("Soil_2023/fc_gNATSGO_US.tif")
    val nlcdInt   = sc.geoTiff[Int]("NLCD_2023")
    val nlcdRDD   = nlcdInt.mapPixels(_.toFloat)
    val elevKV    = enforcePartitionKV(RasterOperationsFocal.reshapeNN(elevRDD,_=>aoiMeta,parts),part)
      .persist(StorageLevel.DISK_ONLY)
    elevKV.foreachPartition(_ => ())

    val awcKV     = enforcePartitionKV(RasterOperationsFocal.reshapeNN(awcRDD,_=>aoiMeta,parts),part)
      .persist(StorageLevel.DISK_ONLY)
    awcKV.foreachPartition(_ => ())

    val fcKV      = enforcePartitionKV(RasterOperationsFocal.reshapeNN(fcRDD,_=>aoiMeta,parts),part)
      .persist(StorageLevel.DISK_ONLY)
    fcKV.foreachPartition(_ => ())

    val nlcdKV    = enforcePartitionKV(RasterOperationsFocal.reshapeNN(nlcdRDD,_=>aoiMeta,parts),part)
      .persist(StorageLevel.DISK_ONLY)
    nlcdKV.foreachPartition(_ => ())

    // Loop over days
    var curr = LocalDate.parse("2023-01-01")
    val end  = LocalDate.parse("2023-01-01")
    // start/end for your Landsat sampling window (±1 month around this date):
    var currLandsat = LocalDate.parse("2021-01-01")
    val endLandsat = LocalDate.parse("2021-01-01")
    var star = curr.toString
    val endDate = end.toString
    var prevOpt: Option[RDD[(Int, ITile[Array[Float]])]] = None
    while(!curr.isAfter(end)){
      prevOpt = Some(processDay(sc,curr,currLandsat,aoi,aoiMeta,fs,parts,part,prevOpt,awcKV,fcKV,nlcdKV,elevKV))
      curr = curr.plusDays(1)
      currLandsat = currLandsat.plusDays(1)
    }

    // Save final composite
    val finalRdd:RasterRDD[Array[Float]] = prevOpt.get.values.mapPixels(arr=>{
      val out = Array.fill(5)(-9999f)
      Array.copy(arr,0,out,0,math.min(arr.length,5))
      out
    })
    val output = "aoidata_rdpro_landsat_ca"
    val output_file = s"$output/landsat-$star-$endDate.nn.tif"
    GeoTiffWriter.saveAsGeoTiff(finalRdd, output_file, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.OutputNumBands -> "5", GeoTiffWriter.BigTiff -> "yes", GeoTiffWriter.FillValue -> "-9999")) //GeoTiffWriter.BigTiff -> "yes"
    val endTime = System.nanoTime()
    println("Total time of whole process : Reshape rasterRDD: " + (endTime - starttime) / 1E9)
    spark.stop()
  }
}
