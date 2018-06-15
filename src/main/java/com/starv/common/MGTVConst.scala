package com.starv.common

/**
  * @author zyx 
  * @date 2018/5/29.
  */
object MGTVConst {
  val HNLT = "HNLT"
  val HNYD = "HNYD"
  val HNDX = "HNDX"
  val C3 = "c3"
  val APK = "apk"
  val SDK = "sdk"

  //校验平台参数的正确性
  def validatePlatform(platform:String): Unit = {
    if (!(HNLT == platform || HNYD == platform || HNDX == platform)) {
        throw new Exception("平台参数错误")
    }

  }
}
