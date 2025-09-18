package com.baitsss.model
import java.time.{LocalDateTime, ZoneOffset, Duration}
// Function to compute NDVI for a given start date
import java.io.File
import java.time.{LocalDate, LocalDateTime, ZoneOffset, Duration}
import java.time.format.DateTimeFormatter
class CalculationNDVI {

  // Stub function: replace with actual implementation to read NDVI from a .tif file.
  def readNDVI(file: File): Double = {
    // For example, you might use a GeoTIFF library to extract the NDVI value.
    // Here we simply return a dummy value.
    0.3  // stub value; in practice, extract NDVI from the file.
  }

  /**
   * Lists Landsat8 files within a given date range.
   *
   * @param startDate the start date (inclusive)
   * @param endDate the end date (inclusive)
   * @param baseFolder the base folder containing Landsat8 data organized as "baseFolder/yyyy-MM-dd/file.tif"
   * @return a sequence of tuples (sceneDate, file) sorted by sceneDate
   */
  def listLandsatFiles(startDate: LocalDate, endDate: LocalDate, baseFolder: String): Seq[(LocalDate, File)] = {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val baseDir = new File(baseFolder)

    if (!baseDir.exists || !baseDir.isDirectory) {
      throw new IllegalArgumentException(s"$baseFolder is not a valid directory")
    }

    // List subdirectories that are named as dates (yyyy-MM-dd)
    val dateDirs = baseDir.listFiles().filter(_.isDirectory)
    val validDirs = dateDirs.flatMap { dir =>
      try {
        val date = LocalDate.parse(dir.getName, dateFormatter)
        if (!date.isBefore(startDate) && !date.isAfter(endDate)) Some((date, dir))
        else None
      } catch {
        case _: Exception => None  // Ignore folders not matching the date format.
      }
    }

    // For each valid directory, list all .tif files and pair them with their date.
    validDirs.flatMap { case (date, dir) =>
      dir.listFiles().filter(file =>
        file.isFile && file.getName.toLowerCase.endsWith(".tif")
      ).map(file => (date, file))
    }.sortBy { case (date, _) => date.toEpochDay }  // Use toEpochDay for ordering.
  }

  /**
   * Computes the NDVI for a given target date.
   *
   * If an exact scene exists for the target date, its NDVI is used.
   * If the target date is outside the available scenes,
   * the NDVI from the nearest scene is returned.
   * Otherwise, linear interpolation is performed between the nearest available scenes.
   *
   * @param targetDate the date for which NDVI is needed
   * @param baseFolder the folder containing Landsat8 data
   * @param startDate the beginning of the period to search for scenes
   * @param endDate the end of the period to search for scenes
   * @return the NDVI value for the target date
   */
  def computeNDVIForDate(targetDate: LocalDate, baseFolder: String, startDate: LocalDate, endDate: LocalDate): Double = {
    // List all Landsat files within the date range.
    val filesByDate = listLandsatFiles(startDate, endDate, baseFolder)

    if (filesByDate.isEmpty) {
      throw new RuntimeException("No Landsat files found in the given date range.")
    }

    // Convert the file dates to LocalDateTime (assuming scene time at start of day) and read NDVI values.
    // reshape each, stack two time
    val ndviData: Seq[(LocalDateTime, Double)] = filesByDate.map { case (date, file) =>
      (date.atStartOfDay(), readNDVI(file))
    }

    val targetDateTime = targetDate.atStartOfDay()

    // Handle boundary conditions.
    val earliestScene = ndviData.minBy { case (dt, _) => dt.toEpochSecond(ZoneOffset.UTC) }
    val latestScene   = ndviData.maxBy { case (dt, _) => dt.toEpochSecond(ZoneOffset.UTC) }

    if (targetDateTime.isBefore(earliestScene._1)) {
      println(s"Target date $targetDate is before the earliest scene (${earliestScene._1}). Using its NDVI = ${earliestScene._2}.")
      return earliestScene._2
    }

    if (targetDateTime.isAfter(latestScene._1)) {
      println(s"Target date $targetDate is after the latest scene (${latestScene._1}). Using its NDVI = ${latestScene._2}.")
      return latestScene._2
    }

    // Check for an exact match.
    ndviData.find { case (dt, _) => dt.equals(targetDateTime) } match {
      case Some((_, ndvi)) =>
        println(s"Exact match found for $targetDate, NDVI = $ndvi")
        return ndvi
      case None =>
      // Proceed with interpolation.
    }

    // Get available data before and after the target date.
    val beforeData = ndviData.filter { case (dt, _) => dt.isBefore(targetDateTime) }
    val afterData  = ndviData.filter { case (dt, _) => dt.isAfter(targetDateTime) }

    // At this point, both beforeData and afterData should be non-empty (boundaries handled above).
    val (prevDateTime, ndviPrev) = beforeData.maxBy { case (dt, _) => dt.toEpochSecond(ZoneOffset.UTC) }
    val (nextDateTime, ndviNext) = afterData.minBy { case (dt, _) => dt.toEpochSecond(ZoneOffset.UTC) }

    // Compute the time difference in seconds.
    val totalIntervalSeconds = Duration.between(prevDateTime, nextDateTime).getSeconds.toDouble
    val elapsedSeconds = Duration.between(prevDateTime, targetDateTime).getSeconds.toDouble

    // Compute the interpolation fraction.
    val fraction = elapsedSeconds / totalIntervalSeconds

    // Linear interpolation.
    val interpolatedNDVI = ndviPrev + fraction * (ndviNext - ndviPrev)

    println(s"Interpolated NDVI for $targetDate:")
    println(s"  Previous scene: $prevDateTime, NDVI = $ndviPrev")
    println(s"  Next scene:     $nextDateTime, NDVI = $ndviNext")
    println(s"  Fraction elapsed: $fraction")
    println(s"  Interpolated NDVI: $interpolatedNDVI")

    interpolatedNDVI
  }





}
