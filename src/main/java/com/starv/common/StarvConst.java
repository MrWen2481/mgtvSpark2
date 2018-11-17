package com.starv.common;

/**
 * @author zyx
 * @date 2018/3/31.
 */
public class StarvConst {

    private static final String HNDX = "HNDX";
    private static final String APK = "apk";
    private static final String HNDX_APK = "HNDX_APK";
    private static final String C3 = "c3";
    private static final String HNDX_C3 = "HNDX_C3";
    private static final String ALL = "all";
    private static final String HNDX_ALL = "HNDX_ALL";
    private static final String HNLT = "HNLT";
    private static final String HNYD = "HNYD";
    private static final String HNLT_ALL = "HNLT_ALL";
    private static final String HNYD_ALL = "HNYD_ALL";
    private static final String SDK = "sdk";
    private static final String HNLT_SDK = "HNLT_SDK";
    private static final String HNLT_APK = "HNLT_APK";
    private static final String HNYD_SDK = "HNYD_SDK";
    private static final String HNYD_APK = "HNYD_APK";

    /**
     * @param platform             hive平台字段
     * @param originSourceTypeName hive数据源字段
     * @return mysql数据源字段
     */
    public static String getSourceTypeName(String platform, String originSourceTypeName) {
        if (HNDX.equals(platform) && APK.equals(originSourceTypeName)) {
            return HNDX_APK;
        } else if (HNDX.equals(platform) && C3.equals(originSourceTypeName)) {
            return HNDX_C3;
        } else if (HNDX.equals(platform) && ALL.equals(originSourceTypeName)) {
            return HNDX_ALL;
        } else if (HNDX.equals(platform) && originSourceTypeName == null) {
            return HNDX_ALL;
        } else if (HNLT.equals(platform) && originSourceTypeName == null) {
            return HNLT_ALL;
        } else if (HNYD.equals(platform) && originSourceTypeName == null) {
            return HNYD_ALL;
        } else if (HNLT.equals(platform) && ALL.equals(originSourceTypeName)) {
            return HNLT_ALL;
        } else if (HNLT.equals(platform) && SDK.equals(originSourceTypeName)) {
            return HNLT_SDK;
        } else if (HNLT.equals(platform) && APK.equals(originSourceTypeName)) {
            return HNLT_APK;
        } else if (HNYD.equals(platform) && SDK.equals(originSourceTypeName)) {
            return HNYD_SDK;
        } else if (HNYD.equals(platform) && APK.equals(originSourceTypeName)) {
            return HNYD_APK;
        } else if (HNYD.equals(platform) && ALL.equals(originSourceTypeName)) {
            return HNYD_ALL;
        } else {
            throw new RuntimeException("匹配不到!!!");
        }
    }

}
