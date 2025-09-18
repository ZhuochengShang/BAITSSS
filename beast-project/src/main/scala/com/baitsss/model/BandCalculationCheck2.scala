package com.baitsss.model

import com.baitsss.model.BandCalculationCheck.IterativeCalculation
import edu.ucr.cs.bdlab.beast.RasterRDD
import edu.ucr.cs.bdlab.beast.geolite.ITile
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal
import org.apache.spark.rdd.RDD

object BandCalculationCheck2 {

    /**
     * Etsum_n
     * precip_sum_n
     * precip_prism_sum_n
     * irri_sum_n
     * var ET_sum = 0
     * var precip_prism_sum = 0
     * var irri_sum = 0
     * */

    // Helper function to pad an array with a default value if it's shorter than expected.
      def padArray(arr: Array[Float], expectedLength: Int, default: Float = 0.0f): Array[Float] = {
        if (arr.length >= expectedLength) arr
        else arr ++ Array.fill(expectedLength - arr.length)(default)
      }

      def safeBand(band: Array[Float], index: Int, default: Float = 0.0f): Float = {
        if (index < band.length && !band(index).isNaN) band(index) else default
      }

  def IterativeCalculationPre(raster: RasterRDD[Array[Float]],  previousCycle: RasterRDD[Array[Float]]):RasterRDD[Array[Float]] = {
    var overlay: RasterRDD[Array[Float]] = if (previousCycle == null) raster else RasterOperationsLocal.overlay(raster,previousCycle)
    RasterOperationsLocal.mapPixels(overlay, (band: Array[Float]) => { // todo: save to disk and read back, stage?
      // Ensure the input band array has the expected length of 16.
      val paddedBand = padArray(band, 16, 0f)

      val Soil_awc = safeBand(paddedBand, 0)
      val Soil_fc = safeBand(paddedBand, 1)
      val nlcd_u = safeBand(paddedBand, 2)
      val elev_array = safeBand(paddedBand, 3)
      val precip_prism = safeBand(paddedBand, 4)
      val precip = precip_prism // Using precip_prism as precip
      val NDVI = safeBand(paddedBand, 5)
      val LAI = safeBand(paddedBand, 6)
      val Tair_oC = safeBand(paddedBand, 7)
      val S_hum = safeBand(paddedBand, 8)
      val uz_in = safeBand(paddedBand, 9)
      val In_short = safeBand(paddedBand, 10)
      val ET_sum = safeBand(paddedBand, 11)
      val precip_prism_sum = safeBand(paddedBand, 12)
      val irri_sum = safeBand(paddedBand, 13)
      val soilm_sur_pre = safeBand(paddedBand, 14)
      val soilm_root_pre = safeBand(paddedBand, 15)

      // Call the iterative calculation function:
      val res = BandCalculationCheck_test.IterativeCalculation(
        NDVI, LAI, Soil_awc, Soil_fc, nlcd_u, precip, elev_array,
        Tair_oC, S_hum, uz_in, In_short, ET_sum, precip_prism_sum, irri_sum, soilm_sur_pre, soilm_root_pre
      )
      res
    })
  }

  def IterativeCalculationPre2(raster: RDD[(Int,ITile[Array[Float]])]): RDD[(Int,ITile[Array[Float]])] = {
    RasterOperationsLocal.mapPixelsPreservePartitionerKeyedEager(raster, (band: Array[Float]) => { // todo: save to disk and read back, stage?
      // Ensure the input band array has the expected length of 16.
      val paddedBand = padArray(band, 16, 0f)

      // Expected band order:
      // 0: Soil_awc, 1: Soil_fc, 2: nlcd_u, 3: precip_prism, 4: elev_array,
      // 5: Tair_oC, 6: S_hum, 7: uz_in, 8: In_short, 9: NDVI, 10: LAI,
      // 11: ET_sum, 12: precip_prism_sum, 13: irri_sum

      val Soil_awc = safeBand(paddedBand, 0)
      val Soil_fc = safeBand(paddedBand, 1)
      val nlcd_u = safeBand(paddedBand, 2)
      val elev_array = safeBand(paddedBand, 3)
      val precip_prism = safeBand(paddedBand, 4)
      val precip = precip_prism
      val NDVI = safeBand(paddedBand, 5)
      val LAI = safeBand(paddedBand, 6)
      val Tair_oC = safeBand(paddedBand, 7)
      val S_hum = safeBand(paddedBand, 8)
      val uz_in = safeBand(paddedBand, 9)
      val In_short = safeBand(paddedBand, 10)
      val ET_sum = safeBand(paddedBand, 11)
      val precip_prism_sum = safeBand(paddedBand, 12)
      val irri_sum = safeBand(paddedBand, 13)
      val soilm_sur_pre = safeBand(paddedBand, 14)
      val soilm_root_pre = safeBand(paddedBand, 15)

      // Call the iterative calculation function:
      val res = BandCalculationCheck_test.IterativeCalculation(
        NDVI, LAI, Soil_awc, Soil_fc, nlcd_u, precip, elev_array,
        Tair_oC, S_hum, uz_in, In_short, ET_sum, precip_prism_sum, irri_sum, soilm_sur_pre, soilm_root_pre
      )
      res
    })
  }

  def IterativeCalculationPre3(raster: RDD[(Int, ITile[Array[Float]])], previousCycle: RasterRDD[Array[Float]] = null): RDD[(Int, ITile[Array[Float]])] = {
    RasterOperationsLocal.mapPixelsPreservePartitionerKeyed(raster, (band: Array[Float]) => { // todo: save to disk and read back, stage?
      // Ensure the input band array has the expected length of 16.
      val paddedBand = padArray(band, 16, 0f)

      // Expected band order:
      // 0: Soil_awc, 1: Soil_fc, 2: nlcd_u, 3: precip_prism, 4: elev_array,
      // 5: Tair_oC, 6: S_hum, 7: uz_in, 8: In_short, 9: NDVI, 10: LAI,
      // 11: ET_sum, 12: precip_prism_sum, 13: irri_sum

      val Soil_awc = safeBand(paddedBand, 0)
      val Soil_fc = safeBand(paddedBand, 1)
      val nlcd_u = safeBand(paddedBand, 2)
      val elev_array = safeBand(paddedBand, 3)
      val precip_prism = safeBand(paddedBand, 4)
      val precip = precip_prism // Using precip_prism as precip
      val NDVI = safeBand(paddedBand, 5)
      val LAI = safeBand(paddedBand, 6)
      val Tair_oC = safeBand(paddedBand, 7)
      val S_hum = safeBand(paddedBand, 8)
      val uz_in = safeBand(paddedBand, 9)
      val In_short = safeBand(paddedBand, 10)
      val ET_sum = safeBand(paddedBand, 11)
      val precip_prism_sum = safeBand(paddedBand, 12)
      val irri_sum = safeBand(paddedBand, 13)
      val soilm_sur_pre = safeBand(paddedBand, 14)
      val soilm_root_pre = safeBand(paddedBand, 15)

      // Call the iterative calculation function:
      val res = BandCalculationCheck_test.IterativeCalculation(
        NDVI, LAI, Soil_awc, Soil_fc, nlcd_u, precip, elev_array,
        Tair_oC, S_hum, uz_in, In_short, ET_sum, precip_prism_sum, irri_sum, soilm_sur_pre, soilm_root_pre
      )
      res
    })
  }

  def IterativeCalculationSimple(raster: RDD[(Int, ITile[Array[Float]])], previousCycle: RasterRDD[Array[Float]] = null): RDD[(Int, ITile[Array[Float]])] = {
    RasterOperationsLocal.mapPixelsPreservePartitionerKeyedEager(raster, (band: Array[Float]) => { // todo: save to disk and read back, stage?
      // Ensure the input band array has the expected length of 16.
      val paddedBand = padArray(band, 16, 0f)

      // Expected band order:
      // 0: Soil_awc, 1: Soil_fc, 2: nlcd_u, 3: precip_prism, 4: elev_array,
      // 5: Tair_oC, 6: S_hum, 7: uz_in, 8: In_short, 9: NDVI, 10: LAI,
      // 11: ET_sum, 12: precip_prism_sum, 13: irri_sum

      val Soil_awc = safeBand(paddedBand, 0)
      val Soil_fc = safeBand(paddedBand, 1)
      val nlcd_u = safeBand(paddedBand, 2)
      val elev_array = safeBand(paddedBand, 3)
      val precip_prism = safeBand(paddedBand, 4)
      val precip = precip_prism
      val NDVI = safeBand(paddedBand, 5)
      val LAI = safeBand(paddedBand, 6)
      val Tair_oC = safeBand(paddedBand, 7)
      val S_hum = safeBand(paddedBand, 8)
      val uz_in = safeBand(paddedBand, 9)
      val In_short = safeBand(paddedBand, 10)
      val ET_sum = safeBand(paddedBand, 11)
      val precip_prism_sum = safeBand(paddedBand, 12)
      val irri_sum = safeBand(paddedBand, 13)
      val soilm_sur_pre = safeBand(paddedBand, 14)
      val soilm_root_pre = safeBand(paddedBand, 15)

      // Call the iterative calculation function:
      val res = Array(0.0f, 0.0f, 0.0f, 0.0f, 0.0f)
      res(0) = precip + NDVI +LAI / 0.00005f
      res(1) = Soil_awc + Soil_fc - LAI
      res(2) = nlcd_u / precip_prism + NDVI + LAI + 0.9f
      res(3) = precip_prism + NDVI + LAI + 0.9f * 0.002f
      res(4) = NDVI * LAI * 0.003f + nlcd_u / Soil_fc
      res
    })
  }


}
