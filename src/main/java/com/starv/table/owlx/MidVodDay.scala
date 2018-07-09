package com.starv.table.owlx
/**
 * @author zyx
 *  2018/5/30.
 */
case class MidVodDay (
                       uuid:String,
                       regionid:String,
                       play_start_time:String,
                       play_end_time:String,
                       media_id:String,
                       var media_name:String,
                       var category_id:String,
                       apk_version:String,
                       media_uuid:String,
                       var channel_id:String,
                       dt:String,
                       platform:String,
                       source_type:String
)