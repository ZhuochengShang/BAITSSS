package com.baitsss.model

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI
import scala.collection.mutable.ArrayBuffer

object WorkflowComparison {
  // Stage count listener
  class StageCountListener extends SparkListener {
    private val stageCounter = new java.util.concurrent.atomic.AtomicInteger(0)

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      stageCounter.incrementAndGet()
    }

    def reset(): Unit = stageCounter.set(0)

    def stageCount: Int = stageCounter.get()
  }

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

  def main(args: Array[String]): Unit = {
    // 1) Spark setup
    val conf = new SparkConf()
      .setAppName("WorkflowNoPersist")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .set("spark.cleaner.referenceTracking.blocking", "true")
      .set("spark.storage.cleaner.enabled", "true")
      .set("spark.cleaner.ttl", "3600")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // 2) Fixed configuration
    val hours = 5

    // 3) Create static 10 GB base RDD
    val parts = 128
    val cellsPerPart = 40960 / 2 // ~5.24 M arrays × 2 KB ≈ 10 GB
    println(s"Building static ~10 GB baseRDD ($parts partitions × $cellsPerPart records)")

    // 4) Create stage listener
    val stageListener = new StageCountListener
    sc.addSparkListener(stageListener)

    // Track size history
    val sizeHistory = ArrayBuffer[(Int, Long, Long, Double, Double)]()

    // Create base RDD (NO PERSIST)
    val baseRDD = makeBigRDD(sc, parts, cellsPerPart)
    val baseCount = baseRDD.count() // materialize once
    val baseSampleSize = baseRDD.first().length
    println(s"Base RDD materialized: $baseCount arrays of $baseSampleSize doubles")

    // 5) Initialize prev = baseRDD
    var prev: RDD[Array[Double]] = baseRDD

    // 6) Hourly loop
    for (hour <- 1 to hours) {
      println(s"\n=== [Hour $hour] Starting combine ===")
      stageListener.reset()

      // a) Reload counts (recomputes from scratch)
      println(f"  baseRDD count = ${baseRDD.count()}%d")
      println(f"  prev count = ${prev.count()}%d")

      // b) Combine: zip + add (NO PERSIST)
      val combined = baseRDD.zip(prev)
        .map { case (b, p) => addArrays(b, p) }
      val combinedCount = combined.count()
      println(s"  Combined count: $combinedCount")

      println(s"  DAG before repartition: depth = ${estimateDAGDepth(combined)}")
      println(s"[Cycle $hour] combined.toDebugString BEFORE repartition:\n${combined.toDebugString}")

      // c) Shuffle: repartition (NO PERSIST)
      val shuffled = combined
        .repartition(parts)
      val shuffledCount = shuffled.count()
      println(s"  Shuffled count: $shuffledCount")

      println(s"  DAG after repartition: depth = ${estimateDAGDepth(shuffled)}")
      println(s"[Cycle $hour] shuffled.toDebugString AFTER repartition:\n${shuffled.toDebugString}")

      println(s"[Stage Count] Hour $hour completed in ${stageListener.stageCount} stages")

      // Set up for next iteration (no unpersist needed)
      prev = shuffled
      println(s"End of Hour $hour - using RDD ${prev.id} for next cycle")
    }

    // 7) Final summary
    println("\n=== Summary ===")
    println("This workflow ran WITHOUT any persistence or saveAsObjectFile")
    println("Final DAG depth: " + estimateDAGDepth(prev))
    println("Final lineage example (first 20 lines):")
    prev.toDebugString.split("\n").take(20).foreach(println)

    println("\n✅ Workflow complete WITHOUT persistence. Lineage grew naturally.")
    spark.stop()
  }
}