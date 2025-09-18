package com.baitsss.model

import com.baitsss.model.Persisit_test.{printHeapUsage, printStorageInfo}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.nio.file.Files
import java.time.LocalDateTime
import scala.util.Try

object HourlyKMeansTest {
  @transient lazy val log = org.apache.log4j.Logger.getLogger(getClass.getName)


  def euclideanDist(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt(a.zip(b).map { case (x, y) => (x - y) * (x - y) }.sum)
  }

  def addPoints(a: Array[Double], b: Array[Double]): Array[Double] = {
    a.zip(b).map { case (x, y) => x + y }
  }

  def avgPoint(sum: Array[Double], count: Long): Array[Double] = {
    sum.map(_ / count)
  }

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
    sc.setCheckpointDir("landsat_checkpoints")
    sc.addSparkListener(new StageCountListener)

    val fs = FileSystem.get(sc.hadoopConfiguration)

    // ─── Accumulators to check caching ───────────────────────────────────────────
    val loadCounter = sc.longAccumulator("loadCounter")
    val transformCounter = sc.longAccumulator("transformCounter")

    // ─── Step 0: create a “raster‐style” RDD of Array[Double] blocks ─────────────
    def makeRasterRDD(parts: Int, cellsPerPart: Int): RDD[Array[Double]] = {
      sc.parallelize(1 to parts, parts)
        .flatMap { pid =>
          Iterator.fill(cellsPerPart) {
            loadCounter.add(1)
            // pretend this is one tile’s pixel values
            Array.fill(256)(scala.util.Random.nextDouble)
          }
        }
    }

    // materialize base RDD
    println(s"\n=== [Hour 1] ${LocalDateTime.now()} ===")
    loadCounter.reset();
    transformCounter.reset()
    // In main, replace the “Hour 1” creation with:
    val parts = 256
    val cellsPerPart = 40960 // ~10.5 M records total, ~20 GB
    println(s"Simulating ~${(parts.toLong * cellsPerPart * 256 * 8) / (1024 * 1024 * 1024)} GB of data")
    var prev: RDD[Array[Double]] = makeRasterRDD(parts, cellsPerPart)
      .persist(StorageLevel.DISK_ONLY)
    prev.count()
    println(s"[Hour 1] loadCounter=${loadCounter.value}, transformCounter=${transformCounter.value}")

    // two counts to prove caching
    prev.count()
    println(s"[Hour 1 retry] loadCounter=${loadCounter.value}, transformCounter=${transformCounter.value}")

    // ─── Hourly cycles ─────────────────────────────────────────────────────────────
    for (hour <- 2 to 5) {
      println(s"\n=== [Hour $hour] ${LocalDateTime.now()} ===")
      loadCounter.reset(); transformCounter.reset()

      // 1) shuffle / repartition
      val shuffled = prev.repartition(144)
      val prevID = prev.id

      // 2) apply your “transaction” (e.g., scale + bias)
      val transformed: RDD[Array[Double]] = shuffled.map { arr =>
        transformCounter.add(1)
        arr.map(_ * 1.1 + 0.05)
      }

      // 3) persist + materialize
      transformed.persist(StorageLevel.DISK_ONLY)
      transformed.count()
      println(s"[Hour $hour] after first count: load=${loadCounter.value}, tx=${transformCounter.value}")

      // 4) second count should not re-fire either stage
      transformed.count()
      println(s"[Hour $hour retry] load=${loadCounter.value}, tx=${transformCounter.value}")

      // 5) cut lineage: save & reload
      val tmp = s"/tmp/pt_${hour}_${System.nanoTime}"
      transformed.saveAsObjectFile(tmp)
      val checkpointed = sc.objectFile[Array[Double]](tmp)
        .repartition(144)
        .persist(StorageLevel.DISK_ONLY)
      checkpointed.count() // force
      //fs.delete(new Path(tmp), true)
      val currentID = checkpointed.id
      println(s"[Cycle $hour] RDD created: $currentID from $prevID")
      println(s"[Cycle $hour] current.toDebugString:\n${checkpointed.toDebugString}")

      printStorageInfo(sc)
      printHeapUsage()


      // 6) cleanup previous RDDs
      prev.unpersist(true)
      shuffled.unpersist(true)
      transformed.unpersist(true)

      prev = checkpointed
    }

    println("\n🎉 Test complete. If the retry counts stayed the same, persist worked correctly.")
    sc.stop()
  }

}
