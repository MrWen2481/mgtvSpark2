package gen;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author zyx
 * @date 2018/5/30.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableModel {
    private String packageName;
    private String tableName;
    private List<MetaData> metaDataList = Lists.newArrayList();
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MetaData{
        private String columnName ;
        private String columnType ;
    }
}
