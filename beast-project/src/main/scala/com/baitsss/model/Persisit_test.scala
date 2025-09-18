package com.baitsss.model

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.{RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, ShuffleDependency, SparkConf, SparkContext}
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
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.BasicFileAttributes
import java.util.stream.Collectors
import scala.util.Try
object Persisit_test {
  @transient lazy val log = org.apache.log4j.Logger.getLogger(getClass.getName)



  class StageCountListener extends SparkListener {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val info = stageCompleted.stageInfo
      val submissionTime = info.submissionTime.getOrElse(-1L)
      val completionTime = info.completionTime.getOrElse(-1L)

      val duration = if (submissionTime >= 0 && completionTime >= 0)
        s"${completionTime - submissionTime} ms"
      else
        "N/A"

      println(s">>> Stage ${info.stageId} completed at ${LocalDateTime.now()}, duration: $duration")
    }
  }

  def printStorageInfo(sc: SparkContext): Unit = {
    println(">>>>> Persisted RDDs:")
    val persisted = sc.getPersistentRDDs
    if (persisted.isEmpty) {
      println("No RDDs are currently persisted.")
    } else {
      persisted.foreach { case (id, rdd) =>
        println(s"[RDD $id] Storage Level: ${rdd.getStorageLevel}, Partitions: ${rdd.partitions.length}")
      }
    }
  }

  def getSparkDiskUsage(): Long = {
    val tmpDir = new File(System.getProperty("java.io.tmpdir"))
    val sparkDirs = tmpDir.listFiles()
      .filter(f => f.getName.startsWith("spark") && f.isDirectory)

    sparkDirs.map { dir =>
      Try {
        Files.walk(dir.toPath)
          .filter(Files.isRegularFile(_))
          .mapToLong(Files.size)
          .sum()
      }.getOrElse {
        println(s"[WARN] Skipping inaccessible: ${dir.getAbsolutePath}")
        0L
      }
    }.sum
  }

  def printHeapUsage(): Unit = {
    val rt = Runtime.getRuntime
    val used = (rt.totalMemory() - rt.freeMemory()) / 1e6
    println(f">>> JVM Heap Used: $used%.2f MB")
  }

  def deleteRecursively(path: String): Unit = {
    import java.nio.file._
    try Files.walk(Paths.get(path)).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)
    catch {
      case e: Exception => println(s"[WARN] Failed to delete $path: ${e.getMessage}")
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PersistTest")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    conf.set("spark.ui.enabled", "true")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.setLogLevel("DEBUG")
    sc.setCheckpointDir("landsat_checkpoints")
    sc.addSparkListener(new StageCountListener)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val totalSizeGB = 10
    val partitions = 1000
    val recordSizeBytes = 1024 * 1024 // 1MB
    val recordsPerPartition = (totalSizeGB * 1024).toInt / partitions

    println(s"📦 Creating baseRDD with ~$totalSizeGB GB total data...")
    val baseRDD = sc.parallelize(0 until partitions, partitions).flatMap { part =>
      (0 until recordsPerPartition).iterator.map { _ =>
        Array.fill(recordSizeBytes / 8)(scala.util.Random.nextDouble) // ~1MB
      }
    }.persist(StorageLevel.DISK_ONLY)

    baseRDD.count() // Trigger persist
    println("✅ baseRDD persisted to DISK_ONLY.")
    printStorageInfo(sc)
    printHeapUsage()

    var previous = baseRDD
    var lastDiskUsageBytes: Long = getSparkDiskUsage()

    for (cycle <- 1 to 24) {
      println(s"\n=== [Cycle $cycle] Start @ ${LocalDateTime.now()} ===")

      // Step 1: Cut DAG by materializing previous to disk
      val prevTrimPath = s"/tmp/spark_dagcut_cycle_input_${cycle}_${System.nanoTime()}"
      previous.saveAsObjectFile(prevTrimPath)

      val trimmedPrev = sc.objectFile[Array[Double]](prevTrimPath)
        .repartition(partitions)
        .persist(StorageLevel.DISK_ONLY)
      trimmedPrev.count() // Materialize before delete

      try fs.delete(new Path(prevTrimPath), true)
      catch {
        case e: Exception => println(s"[WARN] Failed to delete $prevTrimPath: ${e.getMessage}")
      }

      val prevID = previous.id
      previous.unpersist(true)
      println(s"[Cycle $cycle] Unpersisted RDD $prevID")

      // Step 2: Create new transformed RDD from lineage-free RDD
      val current = trimmedPrev.mapPartitions(_.map(_.map(_ * 1.01)))

      // Step 3: Save and reload to cut current DAG
      val tmpPath = s"/tmp/spark_dagcut_cycle_output_${cycle}_${System.nanoTime()}"
      current.saveAsObjectFile(tmpPath)

      val trimmed = sc.objectFile[Array[Double]](tmpPath)
        .repartition(partitions)
        .persist(StorageLevel.DISK_ONLY)
      trimmed.count() // Trigger full materialization

      try fs.delete(new Path(tmpPath), true)
      catch {
        case e: Exception => println(s"[WARN] Failed to delete $tmpPath: ${e.getMessage}")
      }

      val currentID = trimmed.id
      println(s"[Cycle $cycle] RDD created: $currentID from $prevID")
      println(s"[Cycle $cycle] current.toDebugString:\n${trimmed.toDebugString}")

      printStorageInfo(sc)
      printHeapUsage()

      val currentDiskUsage = getSparkDiskUsage()
      val delta = (currentDiskUsage - lastDiskUsageBytes) / 1e6
      val sizeMB = currentDiskUsage / 1e6
      println(f"[Cycle $cycle] Spark temp size: $sizeMB%.2f MB (Δ $delta%.2f MB)")
      lastDiskUsageBytes = currentDiskUsage

      previous = trimmed
    }


    println("🎉 Completed all 24 cycles.")
    println("🌐 Spark UI available at http://localhost:4040")
    println("🕓 Sleeping for 10 minutes to allow Spark UI inspection...")
    Thread.sleep(10 * 60 * 1000)
    sc.stop()
  }


}
