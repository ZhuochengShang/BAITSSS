package com.baitsss.model

import scala.util.Random

object BandCalculationCheck_test {
  /**Constant values**/
      // todo: add TimeStamp, previous cycle round RasterRDD[Array[Float]] // todo: create empty Raster
      // todo: add to paper, the large number of intermediate values in this application and all have dependencies
  def IterativeCalculation(NDVI_in: Float, LAI_in: Float, Soil_awc_in: Float, Soil_fc_in: Float,
                           nlcd_u_in: Float, precip_in: Float, elev_array_in: Float,
                           Tair_oC_in: Float, S_hum_in: Float, uz_in_in: Float, In_short_in: Float,
                           ET_sum_in: Float, precip_prism_sum_in: Float, irri_sum_in: Float, soilm_pre_in: Float, soilm_root_pre_in: Float):Array[Float] = {
        // np.where(input_day < 250, 40, 40) // todo, check which day (1 ~ 365)
        val rl_min = 40 //timestamp.getDay

        // name band value
        val NDVI = NDVI_in
        var LAI = LAI_in
        LAI = if (LAI <=0 ) 0.0001f else if (LAI >= 6) 6 else LAI

        var Soil_awc = Soil_awc_in
        var Soil_fc = Soil_fc_in
        var nlcd_u = nlcd_u_in
        var precip = precip_in
        var elev_array = elev_array_in
        var Tair_oC = Tair_oC_in
        var S_hum = S_hum_in
        var uz_in = uz_in_in
        var In_short = In_short_in

        var soilm_pre = soilm_pre_in // todo: overlay previous value
        var soilm_root_pre = soilm_root_pre_in // todo: overlay previous value
        var precip_hour_com = 0.0
        var precip_prism_hour_com = 0.0
        var irri_hour_com = 0.0

        var ET_sum = ET_sum_in
        var precip_sum = 0.0
        var precip_prism_sum = precip_prism_sum_in
        var irri_sum = irri_sum_in

        //Initialization
        var H_Flux_rep_soil_pre = 0.0 // todo: overlay previous value
        var G_Flux_rep_soil_pre = 0.0
        var G_Flux_rep_veg_pre = 0.0
        var H_Flux_rep_veg_pre = 0.0
        var ETveg_sec_pre = 7.0191862e-005
        var ETsoil_sec_pre = 5.089054e-006

        var irri_app = 0.0

        // Start calculation
        val Zom = 0.018 * LAI
        val Theta_ref = Soil_fc / 100.0f
        var Theta_wilt = Theta_ref - Soil_awc
        if (Theta_wilt < 0) {
          Theta_wilt = 0.004f
        }
        val RAW = BAITSSS_Constants.MAD * Soil_awc
        val Thres_mois = Theta_ref - RAW
        val pressureVal = (101.3 * Math.pow(((293 - elev_array * 0.0065) / 293), 5.26))
        val Psyc_con = 0.000665 * pressureVal
        val Tair = Tair_oC + 273.15
        val fc_eqn = (NDVI - BAITSSS_Constants.NDVImin) / (BAITSSS_Constants.NDVImax - BAITSSS_Constants.NDVImin)
        val fc = if (fc_eqn < BAITSSS_Constants.fc_min) {
          BAITSSS_Constants.fc_min
        } else if (fc_eqn > BAITSSS_Constants.fc_max) {
          BAITSSS_Constants.fc_max
        } else {
          fc_eqn
        }
        val Emis_Ta = 1 - 0.261 * Math.exp(-0.000777 * (Tair_oC * Tair_oC)) // (1- 0.261 * np.exp(- 0.000777 * np.power((Tair_oC), 2)))
        //  In_long = (np.power( Tair, 4) * Stefan_Boltzamn(np.power(Tair, 4) * Stefan_Boltzamn) * Emis_Ta) * Emis_Ta
        val In_long = Math.pow(Tair, 4) * BAITSSS_Constants.Stefan_Boltzamn * Emis_Ta
        val es = 0.611 * Math.exp((17.27 * Tair_oC) / (Tair_oC + 237.3))
        val ea = (pressureVal * S_hum) / (0.378 * S_hum + 0.622)
        val hc = 0.15 * LAI
        val d = 1.1 * hc * Math.log(1 + Math.pow((0.2 * LAI), 0.25))
        val n = if (hc < 1) {
          2.5
        } else if (hc > 10) {
          4.25
        } else {
          2.31 + 0.194 * hc
        }
        val Z1 = if (fc <= 0.6) {
          hc - d
        } else if (fc >= BAITSSS_Constants.fc_full_veg) {
          0.1 * Zom
        }
        else {
          (hc - d) - (((hc - d) - (0.1 * Zom)) * (fc - 0.6)) / (1 - 0.6)
        }
        val rac = BAITSSS_Constants.rb / (2 * (LAI / fc))
        var uz = if (uz_in <= 2) 2 else uz_in // todo: do in the previous step
        //(0.41 * 0.41 * uz * (hc - d)) / (np.log((z_b_wind - (d)) / Zom))
        val kh = (0.41 * 0.41 * uz * (hc - d)) / (Math.log((BAITSSS_Constants.z_b_wind - (d)) / Zom))
        val ras_full_act_eqn = (hc * Math.exp(n) * ((Math.exp(-1 * n * (Zom / hc))) - Math.exp(-1 * n * (d + Zom) / hc)) / (n * kh))
        val ras_full_ini = if (ras_full_act_eqn <= 1) {
          1
        } else ras_full_act_eqn
        val ras_bare_ini = (Math.log(BAITSSS_Constants.z_b_wind / BAITSSS_Constants.Zos) * Math.log((d + Zom) / BAITSSS_Constants.Zos)) / (0.41 * 0.41 * uz)
        val ras_ini = 1 / ((fc / ras_full_ini) + ((1 - fc) / ras_bare_ini))
        var ras = if (fc >= BAITSSS_Constants.fc_full_veg) {
          0
        } else if (ras_ini < 1) {
          1
        } else if (ras_ini > 5000) {
          5000
        } else {
          ras_ini
        }
        val Air_den = pressureVal / (1.01 * 0.287 * Tair)
        var u_fri_neu = (0.41 * uz) / Math.log((BAITSSS_Constants.z_b_wind - (d)) / Zom)
        var rah_est = (Math.log((BAITSSS_Constants.z_b_wind - (d)) / Zom) * Math.log((BAITSSS_Constants.z_b_air - (d)) / Z1)) / (0.41 * 0.41 * uz)
        var rah = if (rah_est < BAITSSS_Constants.rahlo) {
          BAITSSS_Constants.rahlo
        } else if (rah_est > BAITSSS_Constants.rahhi) {
          BAITSSS_Constants.rahhi
        } else {
          rah_est
        }
        var soilm_cur = soilm_pre - ((ETsoil_sec_pre * 60 * BAITSSS_Constants.time_step) * (1 - fc) - (precip - BAITSSS_Constants.ssrun)) / BAITSSS_Constants.soil_depth
        var soilm_cur_final = if (soilm_cur <= 0) {
          0.01
        } else if (soilm_cur > Theta_ref) {
          Theta_ref
        }
        else {
          soilm_cur
        }
        var rss_eqn = 3.5 * Math.pow((BAITSSS_Constants.Theta_sat / soilm_cur_final), 2.3) + 33.5
        val rss = if (rss_eqn < 35) {
          35
        } else if (rss_eqn >= 5000) {
          5000
        } else {
          rss_eqn
        }
        var tsurf_eq_soil = ((H_Flux_rep_soil_pre * (ras.+(rah))) / (Air_den * BAITSSS_Constants.Cp)) + Tair
        var Ts = if (In_short <= 100) {
          Tair - 2.5
        } else if (tsurf_eq_soil >= BAITSSS_Constants.tshi) {
          BAITSSS_Constants.tshi
        } else if (tsurf_eq_soil <= BAITSSS_Constants.tslo) {
          BAITSSS_Constants.tslo
        } else {
          tsurf_eq_soil
        }
        var Lambda_soil = (2.501 - 0.00236 * (Ts - 273.15)) * 1000000
        var eosur = 0.611 * Math.exp((17.27 * (Ts - 273.15)) / ((Ts - 273.15) + 237.3))
        var LE_soil_ini = ((eosur - ea) * BAITSSS_Constants.Cp * Air_den) / ((ras.+(rah) + rss) * Psyc_con)
        var ETsoil_sec_eqn = LE_soil_ini / Lambda_soil
        var ETsoil_sec = if (ETsoil_sec_eqn > BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac) {
          BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac
        } else if (ETsoil_sec_eqn < BAITSSS_Constants.ET_min) {
          BAITSSS_Constants.ET_min
        } else {
          ETsoil_sec_eqn
        }
        var ETsoil_sec_ave = (ETsoil_sec_pre + ETsoil_sec) / 2
        var LE_soil = ETsoil_sec_ave * Lambda_soil
        var outlwr_soil = Math.pow((Ts), 4) * BAITSSS_Constants.Emiss_soil * BAITSSS_Constants.Stefan_Boltzamn
        var netrad_soil = In_short - (BAITSSS_Constants.Albedo_soil * In_short) + In_long - outlwr_soil - (1 - BAITSSS_Constants.Emiss_soil) * In_long
        var sheat_soil = netrad_soil - G_Flux_rep_soil_pre - LE_soil
        //Soil heat flux
        var gheat_h = 0.4 * sheat_soil
        var gheat_netrad = 0.15 * netrad_soil
        var gheat_soil = Math.max(gheat_h, gheat_netrad)
        G_Flux_rep_soil_pre = gheat_soil
        //Soil moisture in root zone
        var soilm_root = soilm_root_pre + ((precip + irri_app - BAITSSS_Constants.ssrun) - (ETveg_sec_pre * 60 * BAITSSS_Constants.time_step * fc) - ((ETsoil_sec * 60 * BAITSSS_Constants.time_step * (1 - fc)))) / BAITSSS_Constants.droot
        var soilm_root_limit = if (soilm_root <= 0) {
          0.01
        } else if (soilm_root > Theta_ref) {
          Theta_ref
        } else {
          soilm_root
        }
        var irri_amount = (Theta_ref - soilm_root) * BAITSSS_Constants.droot
        //Irrigation amount and irrigation flag
        var irrigation = if (BAITSSS_Constants.Irri_flag == 1 && soilm_root_limit < Thres_mois && (nlcd_u > 80 && nlcd_u < 83)) {
          irri_amount
        } else {
          0
        }
        //Final soil moisture
        var soilm_root_final = if (BAITSSS_Constants.Irri_flag == 1 && soilm_root_limit < Thres_mois && (nlcd_u > 80 && nlcd_u < 83)) {
          soilm_root_limit + 0.04
        } else {
          soilm_root_limit
        }
        //surface irrigation
        var updated_soim_cur_sprinkler = if (irrigation == irri_amount) {
          soilm_root_final
        } else {
          soilm_cur_final
        }
        var soilm_cur_final_sprinker = updated_soim_cur_sprinkler
        //  todo: soilm_cur_final = np.where(irri_type == 'sprinkler' , soilm_cur_final_sprinker , soilm_cur_final )
        soilm_cur_final = soilm_cur_final_sprinker
        //Canopy temperature
        var tveg_eq = ((H_Flux_rep_veg_pre * (rac.+(rah))) / (Air_den * BAITSSS_Constants.Cp)) + Tair
        var Tc = if (fc <= BAITSSS_Constants.fc_min) {
          Ts
        } else if (In_short <= 100) {
          Tair - 2.5
        } else if (tveg_eq >= BAITSSS_Constants.tshi) {
          BAITSSS_Constants.tshi
        } else if (tveg_eq <= BAITSSS_Constants.tslo) {
          BAITSSS_Constants.tslo
        } else {
          tveg_eq
        }
        //Saturated vapor pressure
        var eoveg = 0.611 * Math.exp((17.27 * (Tc - 273.15)) / ((Tc - 273.15) + 237.3))
        //Latent heat of vaporization
        var Lambda_veg = (2.501 - 0.00236 * (Tc - 273.15)) * 1000000
        //vapor pressure deficit
        var vpd = eoveg - ea
        var f = 0.55 * (In_short / BAITSSS_Constants.Rgl) * (2 / (LAI))
        //Jarvis function related to solar radiation (0 to 1)
        var F1 = ((rl_min / BAITSSS_Constants.rl_max) + f) / (1 + f)
        //Avialble water fraction(0 to 1)
        var soil_fac = (soilm_root_final - Theta_wilt) / (Theta_ref - Theta_wilt)
        var AWF = if (soil_fac < 0) {
          0
        } else if (soil_fac > 1) {
          1
        }
        else {
          soil_fac
        }
        val Wo = 1
        val Wf = 800
        var W = (Wo * Wf) / (Wo + ((Wf - Wo) * Math.exp(-12 * AWF)))
        var F4 = Math.log(W) / Math.log(Wf)
        //Temperature
        val b5 = 0.0016
        var F2 = 1 - b5 * (298 - Tair) * (298 - Tair)
        //Vapor pressure deficit
        val c3 = 0.1914
        var F3_con = 1 - c3 * vpd
        var F3 = if (F3_con < 0.1) {
          0.1
        } else if (F3_con > 1) {
          1
        } else {
          F3_con
        }
        //Canopy resistance
        var rsc = rl_min / ((LAI / fc) * F1 * F2 * F4 * F3)
        var rsc_final = if (rsc <= rl_min) {
          rl_min
        } else if (rsc > BAITSSS_Constants.rl_max) {
          BAITSSS_Constants.rl_max
        } else {
          rsc
        }
        //Transpiration
        var ETveg_sec_eqn = (((eoveg - ea) * BAITSSS_Constants.Cp * Air_den) / (((rsc_final.+(rac) + rah) * Psyc_con)) / Lambda_veg)
        var ETveg_sec = if (ETveg_sec_eqn > BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac) {
          BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac
        } else if (ETveg_sec_eqn < BAITSSS_Constants.ET_min) {
          BAITSSS_Constants.ET_min
        } else {
          ETveg_sec_eqn
        }
        var ETveg_sec_ave = (ETveg_sec_pre + ETveg_sec) / 2
        //Latent heat flux
        var LE_veg = Lambda_veg * ETveg_sec_ave
        var outlwr_veg = Math.pow((Tc), 4) * BAITSSS_Constants.BB_Emissi * BAITSSS_Constants.Stefan_Boltzamn
        //Outgoing longwave radiation
        //Net radiation
        var netrad_veg = In_short - (BAITSSS_Constants.Albedo_veg * In_short) + In_long - outlwr_veg - (1 - BAITSSS_Constants.Emiss_veg) * In_long
        var sheat_1_veg = netrad_veg - LE_veg
        var sheat_veg = if (sheat_1_veg < BAITSSS_Constants.shlo) {
          BAITSSS_Constants.shlo
        } else if (sheat_1_veg > BAITSSS_Constants.shhi) {
          BAITSSS_Constants.shhi
        } else sheat_1_veg
        //Combined section
        var sheat = sheat_veg * fc + (1 - fc) * sheat_soil
        //Monin Obukhov Length
        var L = -1 * (Air_den * BAITSSS_Constants.Cp * Tair * Math.pow((u_fri_neu), 3)) / (sheat * 4.02)
        L = if (L < -500) {
          -500
        } else if (L > 500) 500 else L
        //Monin Obukhov Parameter
        //Correction of momentum and heat
        var X_z_b_wind = Math.pow((1 - ((16 * (BAITSSS_Constants.z_b_wind - d)) / L)), 0.25)
        var eqn51 = 2 * Math.log((1 + X_z_b_wind) / 2) + Math.log((1 + Math.pow((X_z_b_wind), 2)) / 2) - 2 * Math.atan(X_z_b_wind) + 1.5708
        var eqn54 = -5 * BAITSSS_Constants.z_b_wind / L
        var psi_m_z_b_wind = if (L <= 0) eqn51 else eqn54
        var eqn54a = -5 * BAITSSS_Constants.z_b_air / L
        var X_z_b_air = Math.pow((1 - ((16 * (BAITSSS_Constants.z_b_air - d)) / L)), 0.25)
        var eqn52a = 2 * Math.log((1 + Math.pow((X_z_b_air), 2)) / 2)
        var psi_h_z_b_air = if (L <= 0) eqn52a else eqn54a
        //Corrected friction velocity
        var u_fri = (0.41 * uz) / (Math.log((BAITSSS_Constants.z_b_wind - (d)) / Zom) - psi_m_z_b_wind)
        u_fri = if (u_fri < BAITSSS_Constants.u_fri_lo) {
          BAITSSS_Constants.u_fri_lo
        } else if (u_fri > BAITSSS_Constants.u_fri_hi) {
          BAITSSS_Constants.u_fri_hi
        } else u_fri
        //Correction of momentum and heat
        //for partial canopy part of completely bare soil from zom to d + zom
        var X_dzom = Math.pow((1 - ((16 * (d + Zom)) / L)), 0.25)
        var eqn52a_dzom = 2 * Math.log((1 + Math.pow((X_dzom), 2)) / 2)
        var eqn54a_dzom = -5 * (d + Zom) / L
        val psi_h_dzom = if (L <= 0) eqn52a_dzom else eqn54a_dzom
        //This is last part added in eqn.
        var X_hd = Math.pow((1 - ((16 * (hc - d)) / L)), 0.25)
        var eqn52a_hd = 2 * Math.log((1 + Math.pow((X_hd), 2)) / 2)
        var eqn54a_hd = -5 * hc / L
        var psi_h_hd = if (fc >= BAITSSS_Constants.fc_full_veg) {
          0
        } else if (L <= 0) eqn52a_hd else eqn54a_hd
        //Aerodynamic resistance
        val rah_lim = (Math.log((BAITSSS_Constants.z_b_air - d) / Z1) - psi_h_z_b_air + psi_h_hd) / (0.41 * u_fri)
        rah = if (rah_lim < BAITSSS_Constants.rahlo) {
          BAITSSS_Constants.rahlo
        } else if (rah_lim > BAITSSS_Constants.rahhi) {
          BAITSSS_Constants.rahhi
        } else rah_lim

        // Calculate hourly ET components for the current time step
//        ETsoil_sec_final = 1 * ETsoil_sec_pre
//        ETsoil_hour = (1 - fc) * 3600 * ETsoil_sec_ave
//        ETveg_sec_final = 1 * ETveg_sec_pre
//        ETveg_hour = ETveg_sec_ave * 3600 * fc
//        EThour_com = ETveg_sec_ave * 3600 * fc + (1 - fc) * 3600 * ETsoil_sec_ave

        var ETsoil_sec_final = 0.0
        var ETsoil_hour = 0.0
        var ETveg_sec_final = 0.0
        var ETveg_hour = 0.0
        var EThour_com = 0.0
        // iterative 10 times
        for (i <- 1 until 10) {
          rah_est = rah
          ETsoil_sec_pre = ETsoil_sec_ave
          ETveg_sec_pre = ETveg_sec_ave

          val H_Flux_new_soil = (sheat_soil + H_Flux_rep_soil_pre) / 2
          H_Flux_rep_soil_pre = H_Flux_new_soil
          val H_Flux_new_veg = (sheat_veg + H_Flux_rep_veg_pre) / 2
          H_Flux_rep_veg_pre = H_Flux_new_veg
          val u_fri_new = (u_fri + u_fri_neu) / 2
          u_fri_neu = u_fri_new

          //Soil section
          soilm_cur = soilm_pre - ((ETsoil_sec_pre * 60 * BAITSSS_Constants.time_step * (1 - fc)) - (precip - BAITSSS_Constants.ssrun)) / BAITSSS_Constants.soil_depth
          soilm_cur_final = if (soilm_cur <= 0) {
            0.01
          } else if (soilm_cur > Theta_ref) {
            Theta_ref
          }
          else {
            soilm_cur
          }
          rss_eqn = 3.5 * Math.pow((BAITSSS_Constants.Theta_sat / soilm_cur_final), 2.3) + 33.5
          val rss = if (rss_eqn < 35) {
            35
          } else if (rss_eqn >= 5000) {
            5000
          } else {
            rss_eqn
          }
          tsurf_eq_soil = ((H_Flux_rep_soil_pre * (ras.+(rah))) / (Air_den * BAITSSS_Constants.Cp)) + Tair
          Ts = if (In_short <= 100) {
            Tair - 2.5
          } else if (tsurf_eq_soil >= BAITSSS_Constants.tshi) {
            BAITSSS_Constants.tshi
          } else if (tsurf_eq_soil <= BAITSSS_Constants.tslo) {
            BAITSSS_Constants.tslo
          } else {
            tsurf_eq_soil
          }
          Lambda_soil = (2.501 - 0.00236 * (Ts - 273.15)) * 1000000
          eosur = 0.611 * Math.exp((17.27 * (Ts - 273.15)) / ((Ts - 273.15) + 237.3))
          LE_soil_ini = ((eosur - ea) * BAITSSS_Constants.Cp * Air_den) / ((ras.+(rah) + rss) * Psyc_con)
          ETsoil_sec_eqn = LE_soil_ini / Lambda_soil
          ETsoil_sec = if (ETsoil_sec_eqn > BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac) {
            BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac
          } else if (ETsoil_sec_eqn < BAITSSS_Constants.ET_min) {
            BAITSSS_Constants.ET_min
          } else {
            ETsoil_sec_eqn
          }
          ETsoil_sec_ave = (ETsoil_sec_pre + ETsoil_sec) / 2
          LE_soil = ETsoil_sec_ave * Lambda_soil
          outlwr_soil = Math.pow((Ts), 4) * BAITSSS_Constants.Emiss_soil * BAITSSS_Constants.Stefan_Boltzamn
          netrad_soil = In_short - (BAITSSS_Constants.Albedo_soil * In_short) + In_long - outlwr_soil - (1 - BAITSSS_Constants.Emiss_soil) * In_long
          sheat_soil = netrad_soil - G_Flux_rep_soil_pre - LE_soil
          gheat_h = 0.4 * sheat_soil
          gheat_netrad = 0.15 * netrad_soil
          gheat_soil = Math.max(gheat_h, gheat_netrad)
          G_Flux_rep_soil_pre = gheat_soil

          //Soil moisture in root zone
          soilm_root = soilm_root_pre + ((precip + irri_app - BAITSSS_Constants.ssrun) - (ETveg_sec_pre * 60 * BAITSSS_Constants.time_step * fc) - ((ETsoil_sec * 60 * BAITSSS_Constants.time_step * (1 - fc)))) / BAITSSS_Constants.droot
          soilm_root_limit = if (soilm_root <= 0) {
            0.01
          } else if (soilm_root > Theta_ref) {
            Theta_ref
          } else {
            soilm_root
          }
          irri_amount = (Theta_ref - soilm_root) * BAITSSS_Constants.droot
          //Irrigation amount and irrigation flag
          irrigation = if (BAITSSS_Constants.Irri_flag == 1 && soilm_root_limit < Thres_mois && (nlcd_u > 80 && nlcd_u < 83)) {
            irri_amount
          } else {
            0
          }
          //Final soil moisture
          soilm_root_final = if (BAITSSS_Constants.Irri_flag == 1 && soilm_root_limit < Thres_mois && (nlcd_u > 80 && nlcd_u < 83)) {
            soilm_root_limit + 0.04
          } else {
            soilm_root_limit
          }
          //surface irrigation
          updated_soim_cur_sprinkler = if (irrigation == irri_amount) {
            soilm_root_final
          } else {
            soilm_cur_final
          }

          soilm_cur_final_sprinker = updated_soim_cur_sprinkler
          //  todo: soilm_cur_final = np.where(irri_type == 'sprinkler' , soilm_cur_final_sprinker , soilm_cur_final )
          //soilm_cur_final = soilm_cur_final_sprinker
          //Canopy temperature
          tveg_eq = ((H_Flux_rep_veg_pre * (rac.+(rah))) / (Air_den * BAITSSS_Constants.Cp)) + Tair
          Tc = if (fc <= BAITSSS_Constants.fc_min) {
            Ts
          } else if (In_short <= 100) {
            Tair - 2.5
          } else if (tveg_eq >= BAITSSS_Constants.tshi) {
            BAITSSS_Constants.tshi
          } else if (tveg_eq <= BAITSSS_Constants.tslo) {
            BAITSSS_Constants.tslo
          } else {
            tveg_eq
          }
          //Saturated vapor pressure
          eoveg = 0.611 * Math.exp((17.27 * (Tc - 273.15)) / ((Tc - 273.15) + 237.3))
          //Latent heat of vaporization
          //vapor pressure deficit
          vpd = eoveg - ea
          f = 0.55 * (In_short / BAITSSS_Constants.Rgl) * (2 / (LAI))
          F1 = ((rl_min / BAITSSS_Constants.rl_max) + f) / (1 + f)
          //Avialble water fraction(0 to 1)
          soil_fac = (soilm_root_final - Theta_wilt) / (Theta_ref - Theta_wilt)
          val AWF = if (soil_fac < 0) {
            0
          } else if (soil_fac > 1) {
            1
          }
          else {
            soil_fac
          }
          W = (Wo * Wf) / (Wo + ((Wf - Wo) * Math.exp(-12 * AWF)))
          F4 = Math.log((W)) / Math.log(Wf)
          //Temperature
          F2 = 1 - b5 * (298 - Tair) * (298 - Tair)
          //Vapor pressure deficit
          F3_con = 1 - c3 * vpd
          F3 = if (F3_con < 0.1) {
            0.1
          } else if (F3_con > 1) {
            1
          } else {
            F3_con
          }
          //Canopy resistance
          rsc = rl_min / ((LAI / fc) * F1 * F2 * F4 * F3)
          rsc_final = if (rsc <= rl_min) {
            rl_min
          } else if (rsc > BAITSSS_Constants.rl_max) {
            BAITSSS_Constants.rl_max
          } else {
            rsc
          }
          Lambda_veg = (2.501 - 0.00236 * (Tc - 273.15)) * 1000000
          //Transpiration
          ETveg_sec_eqn = (((eoveg - ea) * BAITSSS_Constants.Cp * Air_den) / ((rsc_final + rac.+(rah)) * Psyc_con)) / Lambda_veg
          ETveg_sec = if (ETveg_sec_eqn > BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac) {
            BAITSSS_Constants.Ref_ET * BAITSSS_Constants.Ref_fac
          } else if (ETveg_sec_eqn < BAITSSS_Constants.ET_min) {
            BAITSSS_Constants.ET_min
          } else {
            ETveg_sec_eqn
          }
          ETveg_sec_ave = (ETveg_sec_pre + ETveg_sec) / 2
          //Latent heat flux
          LE_veg = Lambda_veg * ETveg_sec_ave
          outlwr_veg = Math.pow((Tc), 4) * BAITSSS_Constants.BB_Emissi * BAITSSS_Constants.Stefan_Boltzamn
          //Outgoing longwave radiation
          //Net radiation
          netrad_veg = In_short - (BAITSSS_Constants.Albedo_veg * In_short) + In_long - outlwr_veg - (1 - BAITSSS_Constants.Emiss_veg) * In_long
          sheat_1_veg = netrad_veg - LE_veg
          sheat_veg = if (sheat_1_veg < BAITSSS_Constants.shlo) {
            BAITSSS_Constants.shlo
          } else if (sheat_1_veg > BAITSSS_Constants.shhi) {
            BAITSSS_Constants.shhi
          } else sheat_1_veg
          //Combined section
          sheat = sheat_veg * fc + (1 - fc) * sheat_soil

          L = -1 * (Air_den * BAITSSS_Constants.Cp * Tair * Math.pow((u_fri_neu), 3)) / (sheat * 4.02)
          //Monin Obukhov Parameter
          //Correction of momentum and heat
          X_z_b_wind = Math.pow((1 - ((16 * (BAITSSS_Constants.z_b_wind - d)) / L)), 0.25)
          eqn51 = 2 * Math.log((1 + X_z_b_wind) / 2) + Math.log((1 + Math.pow((X_z_b_wind), 2)) / 2) - 2 * Math.atan(X_z_b_wind) + 1.5708
          eqn54 = -5 * BAITSSS_Constants.z_b_wind / L
          psi_m_z_b_wind = if (L <= 0) eqn51 else eqn54
          eqn54a = -5 * BAITSSS_Constants.z_b_air / L
          X_z_b_air = Math.pow((1 - ((16 * (BAITSSS_Constants.z_b_air - d)) / L)), 0.25)
          eqn52a = 2 * Math.log((1 + Math.pow((X_z_b_air), 2)) / 2)
          psi_h_z_b_air = if (L <= 0) eqn52a else eqn54a
          //Corrected friction velocity
          //Correction of momentum and heat
          //for partial canopy part of completely bare soil from zom to d + zom
          X_dzom = Math.pow((1 - ((16 * (d + Zom)) / L)), 0.25)
          eqn52a_dzom = 2 * Math.log((1 + Math.pow((X_dzom), 2)) / 2)
          eqn54a_dzom = -5 * (d + Zom) / L
          //This is last part added in eqn.
          X_hd = Math.pow((1 - ((16 * (hc - d)) / L)), 0.25)
          eqn52a_hd = 2 * Math.log((1 + Math.pow((X_hd), 2)) / 2)
          eqn54a_hd = -5 * hc / L
          psi_h_hd = if (fc >= BAITSSS_Constants.fc_full_veg) {
            0
          } else if (L <= 0) eqn52a_hd else eqn54a_hd
          u_fri = (0.41 * uz) / (Math.log((BAITSSS_Constants.z_b_wind - (d)) / Zom) - psi_m_z_b_wind)
          u_fri = if (u_fri < BAITSSS_Constants.u_fri_lo) {
            BAITSSS_Constants.u_fri_lo
          } else if (u_fri > BAITSSS_Constants.u_fri_hi) BAITSSS_Constants.u_fri_hi else u_fri
          //Aerodynamic resistance
          val rah_con = (Math.log((BAITSSS_Constants.z_b_air - d) / Z1) - psi_h_z_b_air + psi_h_hd) / (0.41 * u_fri)
          rah = if (rah_con < BAITSSS_Constants.rahlo) {
            BAITSSS_Constants.rahlo
          } else if (rah_con > BAITSSS_Constants.rahhi) BAITSSS_Constants.rahhi else rah_con
          //val rah_conv = Math.abs(rah - rah_est)
          //ETsoil_sec_final = 1 * ETsoil_sec_pre
          ETsoil_hour = (1 - fc) * 3600 * ETsoil_sec_ave
          //ETveg_sec_final = 1 * ETveg_sec_pre
          ETveg_hour = ETveg_sec_ave * 3600 * fc
          EThour_com = ETveg_sec_ave * 3600 * fc + (1 - fc) * 3600 * ETsoil_sec_ave
          //ETveg_sec_ave = ETveg_sec_final
          //ETsoil_sec_ave = ETsoil_sec_final
          // Update cumulative sums (precip_hour is set to 0 here; adjust as needed)
        } // end iteration loop

        ET_sum += EThour_com.toFloat
        // Add randomness to ET_sum with bounds
        // Ensure it stays within 0.1 to 0.4 range
        //ET_sum = Math.max(0.1f, Math.min(0.4f, ET_sum_randomized))
        val precip_hour = 0
        precip_sum += precip_hour //todo: check calculation
        precip_prism_sum += precip //precip_prism_hour_com
        irri_sum += irrigation.toFloat //irri_hour_com

        Array(ET_sum.toFloat, precip_prism_sum.toFloat, irri_sum.toFloat, soilm_cur_final.toFloat, soilm_root_final.toFloat)

        //Array(ET_sum.toFloat, precip_prism_sum.toFloat, precip_sum.toFloat, irri_sum.toFloat)
        //Array(ET_sum.toFloat,0.0.toFloat,0.0.toFloat)

      }

}
