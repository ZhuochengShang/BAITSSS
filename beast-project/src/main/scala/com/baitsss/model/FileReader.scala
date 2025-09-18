package com.baitsss.model

import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.{RasterRDD, RasterReadMixinFunctions}
import edu.ucr.cs.bdlab.raptor.{RaptorJoin, RaptorJoinFeature, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FileReader {

  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmm")

  def ListFile(folderPath: String,  AOI: RDD[IFeature], sc: SparkContext): Unit = {

    //NLDAS, Landsat8, NLCD, PRISM, SSURGO and parse each file
    // return RDD(timestamp, filepath)
    val fileSystem = new Path(folderPath).getFileSystem(new Configuration())
    val list_files = fileSystem.listStatus(new Path(folderPath)).filter(_.getPath.getName.toLowerCase.endsWith(".tif"))

  }

  def ListDate_NLDAS_hourly(folder: String): Iterator[(Timestamp, String)] = {
    implicit val timestampOrdering = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }

    // Read data: list all .tif files in the folder.
    val fileSystem = new Path(folder).getFileSystem(new Configuration())
    val list_Nldas = fileSystem.listStatus(new Path(folder))
      .filter(_.getPath.getName.toLowerCase.endsWith(".tif"))

    // Process each file: extract date and hour from filename, keeping only day and hour.
    val NLDAS_RDD_dated: Iterator[(Timestamp, String)] = list_Nldas.map { curr_NLDAS =>
      val fileName = curr_NLDAS.getPath.getName
      val filePath = curr_NLDAS.getPath.toString
      println(s"Processing file: $fileName")

      // Assume filename structure: <prefix>.A<YYYYMMDD>.<HHmm>.<...>.tif
      // e.g., "NLDAS_FORA0125_H.A20230102.1800.002.tif"
      val dateArray = fileName.split("\\.")
      val dateToken = dateArray(1) // e.g., "A20230102"
      val hour = dateArray(2) // e.g., "1800"

      // Remove the leading 'A' to get "20230102"
      val fullDate = dateToken.drop(1)
      // Extract only the day part: the last two characters (e.g., "02")
      val monthPart = fullDate.substring(4, 6)
      val dayPart = fullDate.substring(6, 8)

      // Build a new datetime string using dummy year and month: "1970 01 <day> <hour>"
      // For example, "1970 01 02 1800" for a file from 2023-01-02 18:00.
      val newDateTimeStr = s"2023 $monthPart $dayPart $hour"
      // Define a formatter matching "yyyy MM dd HHmm"
      val formatter = DateTimeFormatter.ofPattern("yyyy MM dd HHmm")
      println(s"Parsed new datetime string (day and hour only): '$newDateTimeStr' from file: $fileName")
      val dateTime = LocalDateTime.parse(newDateTimeStr, formatter)
      val timestamp = Timestamp.valueOf(dateTime)

      (timestamp, filePath)
    }.sortBy(_._1)(timestampOrdering).iterator

    NLDAS_RDD_dated
  }


  def ListDate_NLDAS(folder: String, AOI: RDD[IFeature], sc: SparkContext): Iterator[(Timestamp, RasterRDD[Array[Float]])]  = {

    implicit val timestampOrdering = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }

    // Read data and map with new timestamp
    // get Array of files and then create with Datetime
    val fileSystem = new Path(folder).getFileSystem(new Configuration())
    val list_Nldas = fileSystem.listStatus(new Path(folder)).filter(_.getPath.getName.toLowerCase.endsWith(".tif"))


    val NLDAS_RDD_dated: Iterator[(Timestamp, RasterRDD[Array[Float]])] = list_Nldas.map(curr_NLDAS => { // change to flat map
      val fileName = curr_NLDAS.getPath.getName
      println(fileName)
      val dateArray = fileName.split("\\.")
      val date = dateArray(1)
      val hour = dateArray(2)
      // Remove the leading 'A' and split the date from the hour
      val dateTimePart = date.drop(1) // Drop 'A'
      val datePart = dateTimePart.take(8) // Take the first 8 characters for the date
      // Format as "yyyyMMdd HHmm"
      val formattedDateTime = s"${datePart} ${hour}"
      // Set up the formatter
      val formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmm")
      // Parse the datetime
      val dateTime = LocalDateTime.parse(formattedDateTime, formatter)
      val timestamp = java.sql.Timestamp.valueOf(dateTime)

      val curr_NLDAS_RasterRDD: RDD[ITile[Array[Float]]] = sc.geoTiff(curr_NLDAS.getPath.toString)
      //val NLDAS_RDD_Float: RasterRDD[Array[Float]] = NLDAS_RDD.mapPixels(pixel=>pixel.map(_.toFloat))
      val curr_NLDAS_RasterRDD_float = curr_NLDAS_RasterRDD.mapPixels(_.map(_.toFloat))
      //val NLDAS_Join: RDD[RaptorJoinFeature[Array[Float]]] = RaptorJoin.raptorJoinFeature(curr_NLDAS_RasterRDD_float, AOI)//.map(t => (timestamp, t))
      val NLDAS_obtain_values = curr_NLDAS_RasterRDD_float.mapPixels(p => {
        val values = new Array[Float](4)
        values(0) = p(0) // Tair_oC
        values(1) = p(1) // S_hum
        //wind_array = np.sqrt(wind_u_array ** 2 + wind_v_array ** 2)
        val wind_array = Math.sqrt(p(3) * p(3) + p(4) * p(4)) //uz
        values(2) = wind_array.toFloat
        values(3) = p(10) // In_short
        values
      })
      //val curr_NLDAS_tile_dated = RasterFeatureHelper.appendNewFeature(NLDAS_rasterize, "fileDatetime", timestamp)
      (timestamp, NLDAS_obtain_values)
    }).sortBy(_._1)(timestampOrdering).iterator
    NLDAS_RDD_dated
  }

  def ListDate_NLDAS_files(folder: String, AOI: RDD[IFeature], sc: SparkContext): Iterator[(Timestamp,String)] = {

    implicit val timestampOrdering = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }

    // Read data and map with new timestamp
    // get Array of files and then create with Datetime
    val fileSystem = new Path(folder).getFileSystem(new Configuration())
    val list_Nldas = fileSystem.listStatus(new Path(folder)).filter(_.getPath.getName.toLowerCase.endsWith(".tif"))


    val NLDAS_RDD_dated: Iterator[(Timestamp, String)] = list_Nldas.map(curr_NLDAS => { // change to flat map
      val fileName = curr_NLDAS.getPath.getName
      val filePath = curr_NLDAS.getPath.toString
      println(fileName)
      val dateArray = fileName.split("\\.")
      val date = dateArray(1)
      val hour = dateArray(2)
      // Remove the leading 'A' and split the date from the hour
      val dateTimePart = date.drop(1) // Drop 'A'
      val datePart = dateTimePart.take(8) // Take the first 8 characters for the date
      // Format as "yyyyMMdd HHmm"
      val formattedDateTime = s"${datePart} ${hour}"
      // Set up the formatter
      val formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmm")
      // Parse the datetime
      val dateTime = LocalDateTime.parse(formattedDateTime, formatter)
      val timestamp = java.sql.Timestamp.valueOf(dateTime)


      //val curr_NLDAS_tile_dated = RasterFeatureHelper.appendNewFeature(NLDAS_rasterize, "fileDatetime", timestamp)
      (timestamp, filePath)
    }).sortBy(_._1)(timestampOrdering).iterator
    NLDAS_RDD_dated
  }
}
