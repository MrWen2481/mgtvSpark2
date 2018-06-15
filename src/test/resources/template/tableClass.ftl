package ${packageName}
/**
 * @author zyx
 * @date 2018/5/30.
 */
case class ${tableName} (
  <#list metaDataList as metaData>
     ${metaData.columnName}:${metaData.columnType}<#if metaData_has_next>,</#if>
  </#list>
)