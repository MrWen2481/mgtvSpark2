package com.starv.table.owlx

import com.starv.common.MGTVConst

/**
  * @author zyx
  *         2018/5/30.
  */
case class ResVodDay(
                      uuid: String,
                      regionid: String,
                      play_start_time: String,
                      play_end_time: String,
                      media_id: String,
                      var media_name: String,
                      var category_id: String = "",
                      var category_name: String = "",
                      apk_version: String,
                      var media_uuid: String = "",
                      cp: String = "",
                      pro_id: String = "",
                      var channel_id: String = "",
                      var channel_name: String = "",
                      isfree: String = "0",
                      var flag: String = MGTVConst.VOD_MATCH_CATEGORY_FLAG, //1代表多匹配出来的
                      dt: String,
                      platform: String,
                      source_type: String
                    )