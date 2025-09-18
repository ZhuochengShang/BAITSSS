package com.baitsss.model

import java.time.{Duration, LocalDateTime, ZoneOffset}
// Function to compute NDVI for a given start date
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import scala.collection.JavaConverters._

class LoadLandsat {

  import java.io.File
  import java.time.LocalDate
  import java.time.format.DateTimeFormatter

  /**
   * Finds Landsat8 .tif files (as absolute path strings) for a window that spans one month before and one month after the target date.
   *
   * Assumes that Landsat8 data are stored in subfolders under the baseFolder,
   * with each subfolder named using the "yyyy-MM-dd" format.
   *
   * @param targetDate The target date.
   * @param baseFolder The base folder containing the Landsat8 subfolders.
   * @param thresholdDays The minimum number of days with data expected in the window (default is 10).
   * @return A Map where each key is a LocalDate and its value is a sequence of absolute file path strings.
   */
  def findLandsatFilesForTwoMonthWindowPaths(
                                                  targetDate: LocalDate,
                                                  baseFolder: String,
                                                  thresholdDays: Int = 10,
                                                  hadoopConf: Configuration
                                                ): Map[LocalDate, Seq[String]] = {

    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val startDate = targetDate.minusMonths(1)
    val endDate = targetDate.plusMonths(1)
    val fs = FileSystem.get(hadoopConf)

    val days = Iterator.iterate(startDate)(_.plusDays(1))
      .takeWhile(!_.isAfter(endDate))
      .toList

    val data: Map[LocalDate, Seq[String]] = days.flatMap { date =>
      val folderName = date.format(dateFormatter)
      val dayFolderPath = new Path(s"$baseFolder/$folderName")

      if (fs.exists(dayFolderPath) && fs.isDirectory(dayFolderPath)) {
        val fileStatuses: Array[FileStatus] = fs.listStatus(dayFolderPath)
        val tiffPaths = fileStatuses
          .filter(f => f.isFile && f.getPath.getName.toLowerCase.endsWith(".tif"))
          .map(f => f.getPath.toString)
          .toSeq

        if (tiffPaths.nonEmpty) Some(date -> tiffPaths) else None
      } else {
        None
      }
    }.toMap

    if (data.size < thresholdDays) {
      println(s"Warning: Only found data for ${data.size} days in the window ($startDate to $endDate), which is below the threshold of $thresholdDays days.")
    }

    data
  }

  // Example usage (in REPL or from another function):
  // val targetDate = LocalDate.of(2021, 1, 15)
  // val baseFolder = "/path/to/Landsat8"
  // val filesMap: Map[LocalDate, Seq[String]] = findLandsatFilesForTwoMonthWindowPaths(targetDate, baseFolder)
  // filesMap.foreach { case (date, paths) =>
  //   println(s"$date:")
  //   paths.foreach(println)
  // }


  /*
  Example usage (can be used in REPL or integrated into a larger workflow):

  val targetDate = LocalDate.of(2021, 1, 15)
  val baseFolder = "/path/to/Landsat8"

  // Get the aggregated files for one month before and after the target date.
  val filesMap = findLandsatFilesForTwoMonthWindow(targetDate, baseFolder)

  // Optionally, partition the results into days before and days after the target date.
  val (beforeMap, afterMap) = filesMap.partition { case (date, _) => date.isBefore(targetDate) }

  println("Files from one month before the target date:")
  beforeMap.foreach { case (date, files) =>
    println(s"$date: ${files.map(_.getAbsolutePath).mkString(", ")}")
  }

  println("Files from the target date and one month after:")
  afterMap.foreach { case (date, files) =>
    println(s"$date: ${files.map(_.getAbsolutePath).mkString(", ")}")
  }
  */


}
