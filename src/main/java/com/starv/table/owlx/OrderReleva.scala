package com.starv.table.owlx

case class OrderReleva(
                        state: String,
                        user_id: String,
                        regionid: String,
                        media_name: String,
                        apk_version: String,
                        product_name: String,
                        var pagename: String ="",
                        create_time: String,
                        confirmation: String,
                        status: String,
                        dt: String,
                        platform: String
                      )
