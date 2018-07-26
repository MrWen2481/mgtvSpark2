package com.starv

/**
  * 原始数据中的所有字段
  *
  * @author zyx 
  *  2018/5/29.
  */
case class SourceTmp(
                      //业务状态
                      state: String,
                      user_id: String,
                      create_time: String,
                      regionid: String = "14301",
                      var play_start_time: String = "",
                      var play_end_time: String = "",
                      conf_channel_code: String = "",
                      live_flag: String = "",
                      is_timeshift: String = "0",
                      vod_flag: String = "",
                      play: Boolean = false,
                      manufacturers: String = "",
                      mac: String = "",
                      model: String = "",
                      ip: String = "",
                      system_version: String = "",
                      user_account: String = "",
                      media_id: String = "",
                      media_name: String = "",
                      episodes: String = "",
                      category_id: String = "",
                      sdk_version: String = "",
                      vod_channel_id: String = "",
                      os: String = "",
                      apk_version: String = "",
                      sp_code: String = "",
                      page_id: String = "",
                      pagepath: String = "",
                      nextpagepath: String = "",
                      pagename: String = "",
                      special_id: String = "",
                      way: String = "",
                      offset_name: String = "",
                      offset_id: String = "",
                      key: String = "",
                      keyname: String = "",
                      event_type: String = "",
                      button_id: String = "",
                      button_name: String = "",
                      offset_group: String = "",
                      media_group: String = "",
                      channel_id: String = "",
                      channel_name: String = "",
                      platform: String ="",
                      source_type: String ="",
                      operator: String ="",
                      product_name: String ="",
                      boss_id: String ="",
                      product_price: String ="",
                      status: String ="",
                      confirmation: String ="",
                      error_code: String ="",
                      error_detail: String =""
                    )
