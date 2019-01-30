package com.starv.utils

import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils

/**
  * @author zyx 
  * @date 2018/3/30.
  */
object MGStringUtils {
  private val pattern = Pattern.compile("(.*?\\..*?\\..*?)\\..*?")

  def replaceNullToEmpty(str: String): String = {
    if (StringUtils.isBlank(str) || "null" == str || "NULL" == str) {
      ""
    } else {
      str
    }
  }

  def getParentApkVersion(apkVersion: String): String = {
    if (null == apkVersion || StringUtils.isBlank(apkVersion) || "null" == apkVersion || "NULL" == apkVersion){
      return ""
    }
    val matcher = pattern.matcher(apkVersion)
    if (matcher.find()) {
      return matcher.group(1)
    }
    ""
  }
}
