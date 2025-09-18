package com.baitsss.model

class CalculationLAI {

  // https://dspace.mit.edu/bitstream/handle/1721.1/156996/Wickman-wickman-MENG-CEE-thesis.pdf?sequence=1&isAllowed=y
  def CalculateLAI (NIR: Int, RED: Int): Double = {
    val L = 0.5
    val SAVI = ((1 + L) * NIR - RED) / (L + (NIR + RED));
    var LAI = 6.0
    if (SAVI <= 0.69){
      val c1 = 0.69
      val c2 = 0.59
      val c3 = 0.91
      val value = (-1 * SAVI + c1) / c2
      LAI = -1 * (Math.log(value) / c3)
    }
    if (LAI <= 0)
      LAI = 0.0001
    else if (LAI >= 6)
      LAI = 6.0
    LAI
  }

  def ndviLaiFunc(ndvi: Float): Float = {
    val lai = 7.0f * math.pow(ndvi.toDouble, 3).toFloat
    // Clamp the value between 0 and 6:
    math.max(0f, math.min(6f, lai))
  }

}
