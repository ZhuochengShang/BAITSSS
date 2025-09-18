package com.baitsss.model
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import org.apache.spark.util.LongAccumulator
import org.apache.spark.scheduler._
import scala.collection.mutable.ArrayBuffer

object SyntheticDiskPersistWorkflow {
  // Stage count listener
  class StageCountListener extends SparkListener {
    private val stageCounter = new java.util.concurrent.atomic.AtomicInteger(0)

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      stageCounter.incrementAndGet()
    }

    def reset(): Unit = stageCounter.set(0)

    def stageCount: Int = stageCounter.get()
  }

  // Track size history
  val sizeHistory = ArrayBuffer[(Int, Long, Long, Double, Double)]()

  // Create large RDD
  def makeBigRDD(sc: SparkContext, partitions: Int, recordsPerPartition: Int): RDD[Array[Double]] = {
    sc.parallelize(0 until partitions, partitions).flatMap { _ =>
      (0 until recordsPerPartition).map { _ =>
        Array.fill(256)(scala.util.Random.nextDouble())
      }
    }
  }

  // Add arrays element-wise
  def addArrays(a: Array[Double], b: Array[Double]): Array[Double] = {
    require(a.length == b.length, s"Array length mismatch: ${a.length} vs ${b.length}")
    val result = new Array[Double](a.length)
    var i = 0
    while (i < a.length) {
      result(i) = a(i) + b(i)
      i += 1
    }
    result
  }

  // Estimate DAG depth
  def estimateDAGDepth(rdd: RDD[_]): Int = {
    def getDepth(r: RDD[_], visited: scala.collection.mutable.Set[Int]): Int = {
      if (visited.contains(r.id)) 0
      else {
        visited += r.id
        val parentDepths = r.dependencies.map(_.rdd).map(getDepth(_, visited))
        if (parentDepths.isEmpty) 1
        else 1 + parentDepths.max
      }
    }
    getDepth(rdd, scala.collection.mutable.Set[Int]())
  }

  // Delete directory helper
  def deleteDir(fs: FileSystem, path: Path): Unit = {
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  // Truncate lineage using saveAsObjectFile - FIXED VERSION
  def truncateLineageWithSave(
                               rdd: RDD[Array[Double]],
                               sc: SparkContext,
                               hour: Int,
                               parts: Int,
                               hdfsBase: String
                             ): RDD[Array[Double]] = {
    val savePath = s"$hdfsBase/lineage-hour-$hour"

    println(s"  Saving RDD to break lineage: $savePath")

    // Calculate sum before saving for integrity check
    val sumBefore = rdd.map(_.sum).reduce(_ + _)
    println(f"  Sum before save: $sumBefore%.2f")

    // Save the RDD
    rdd.saveAsObjectFile(savePath)

    // Check saved size
    val fs = FileSystem.get(new URI(hdfsBase), sc.hadoopConfiguration)
    val savedSize = fs.getContentSummary(new Path(savePath)).getLength
    println(f"  Saved size: ${savedSize}%,d bytes (${savedSize / (1024*1024*1024.0)}%.2f GB)")

    // Load back as a simple objectFile without repartition initially
    // This creates a HadoopRDD with minimal dependencies
    println(s"  Loading back from disk...")
    val loaded = sc.objectFile[Array[Double]](savePath)

    // IMPORTANT: Create a completely new RDD by mapping identity
    // This forces Spark to create a new RDD with no parent dependencies
    val mapped = loaded.map(x => x) // Identity map to break all dependencies

    // Now repartition the mapped RDD
    val fresh = mapped
      .repartition(parts)
      .persist(StorageLevel.DISK_ONLY)

    val freshCount = fresh.count()
    val freshSampleSize = fresh.first().length
    println(s"  Fresh RDD: $freshCount arrays of $freshSampleSize doubles")

    // Verify data integrity
    val sumAfter = fresh.map(_.sum).reduce(_ + _)
    println(f"  Sum after reload: $sumAfter%.2f")

    if (Math.abs(sumBefore - sumAfter) > 0.001) {
      println("  ERROR: Data integrity check failed!")
    } else {
      println("  ✅ Data integrity verified")
    }

    // Track size history
    sizeHistory += ((hour, freshCount, savedSize, sumBefore, sumAfter))

    fresh
  }

  def main(args: Array[String]): Unit = {
    // 1) Spark setup
    val conf = new SparkConf()
      .setAppName("SyntheticDiskPersistWorkflow")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .set("spark.cleaner.referenceTracking.blocking", "true")
      .set("spark.storage.cleaner.enabled", "true")
      .set("spark.cleaner.ttl", "3600")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // 2) HDFS setup
    //val hdfsBase = "hdfs://ec-hn.cs.ucr.edu:8040/user/zshan011/workflow-data"
    val hdfsBase = "file:///Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/workflow-data"

    val fs = FileSystem.get(new URI(hdfsBase), sc.hadoopConfiguration)

    // Create workflow directory
    val workflowPath = new Path(hdfsBase)
    if (!fs.exists(workflowPath)) {
      fs.mkdirs(workflowPath)
      println(s"Created workflow directory: $hdfsBase")
    }

    // 3) Create stage listener
    val stageListener = new StageCountListener
    sc.addSparkListener(stageListener)

    // 4) Create static 10 GB base RDD
    val parts = 128
    val cellsPerPart = 40960 / 4 // ~5.24 M arrays × 2 KB ≈ 10 GB
    println(s"Building static ~2 GB baseRDD ($parts partitions × $cellsPerPart records)")

    val baseRDD = makeBigRDD(sc, parts, cellsPerPart)
      .persist(StorageLevel.DISK_ONLY)
    val baseCount = baseRDD.count() // materialize once to disk
    val baseSampleSize = baseRDD.first().length
    println(s"Base RDD materialized: $baseCount arrays of $baseSampleSize doubles")

    // 5) Show persisted RDDs
    println(">> Persisted RDDs:")
    sc.getPersistentRDDs.values.foreach { r =>
      println(s"  - RDD ${r.id}: ${r.getStorageLevel}")
    }

    // 6) Initialize prev = baseRDD
    var prev: RDD[Array[Double]] = baseRDD

    // 7) Hourly loop
    for (hour <- 1 to 5) {
      println(s"\n=== [Hour $hour] Starting combine ===")
      stageListener.reset()

      // a) Reload counts (reads from disk)
      println(f"  baseRDD count = ${baseRDD.count()}%d")
      println(f"  prev count = ${prev.count()}%d")

      // b) Combine: zip + add
      val combined = baseRDD.zip(prev)
        .map { case (b, p) => addArrays(b, p) }
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val combinedCount = combined.count()
      println(s"  Combined count: $combinedCount")

      println(s"  DAG before repartition: depth = ${estimateDAGDepth(combined)}")
      println(s"[Cycle $hour] combined.toDebugString BEFORE repartition:\n${combined.toDebugString}")

      // c) Shuffle: repartition
      val shuffled = combined
        .repartition(parts)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val shuffledCount = shuffled.count()
      println(s"  Shuffled count: $shuffledCount")

      println(s"  DAG before save: depth = ${estimateDAGDepth(shuffled)}")
      println(s"[Cycle $hour] shuffled.toDebugString BEFORE save:\n${shuffled.toDebugString}")

      // d) Save and reload to truncate lineage
      val fresh = truncateLineageWithSave(shuffled, sc, hour, parts, hdfsBase)

      println(s"  DAG after reload: depth = ${estimateDAGDepth(fresh)}")
      println(s"  Fresh RDD dependencies: ${fresh.dependencies.size}")
      fresh.dependencies.foreach { dep =>
        println(s"    - ${dep.getClass.getSimpleName}")
      }
      println(s"[After reload] ${fresh.toDebugString}")

      // Verify lineage was truncated
      if (fresh.dependencies.size <= 1 && estimateDAGDepth(fresh) <= 2) {
        println("  ✅ Lineage successfully truncated!")
      } else {
        println("  ⚠️ Warning: Lineage may not be fully truncated")
      }

      println(s"[Stage Count] Hour $hour completed in ${stageListener.stageCount} stages")

      // e) Cleanup intermediates
      println("Cleaning up intermediate RDDs")
      combined.unpersist(true)
      prev.unpersist(true)
      shuffled.unpersist(true)

      // f) Optional: Clean up saved files from previous hours
      if (hour > 1) {
        val oldPath = new Path(s"$hdfsBase/lineage-hour-${hour-1}")
        if (fs.exists(oldPath)) {
          fs.delete(oldPath, true)
          println(s"  Cleaned up old save from hour ${hour-1}")
        }
      }

      // g) Set up for next iteration
      prev = fresh
      println(s"End of Hour $hour - using fresh RDD ${prev.id} for next cycle")
    }

    // 8) Final summary
    println("\n=== Summary ===")
    println("Hour | Array Count | Saved Size (bytes) | Sum Before | Sum After")
    println("-----|-------------|-------------------|------------|----------")
    sizeHistory.foreach { case (hour, count, size, sumBefore, sumAfter) =>
      println(f"$hour%4d | $count%11d | $size%,18d | $sumBefore%10.2f | $sumAfter%9.2f")
    }


    // 9) Final cleanup
    prev.unpersist(true)
    println("\nFinal cleanup: deleting workflow directory")
    deleteDir(fs, workflowPath)

    // Add this before spark.stop()
    println("Keeping application alive for UI inspection...")
    Thread.sleep(600000) // 10 minutes

    println("\n✅ Workflow complete. baseRDD remained on disk; each hour's output fed into the next.")
    spark.stop()
  }
}