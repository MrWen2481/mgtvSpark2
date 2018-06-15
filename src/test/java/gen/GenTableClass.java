package gen;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.SneakyThrows;

import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 懒人版代码生成器
 * @author zyx
 * @date 2018/5/30.
 */
public class GenTableClass {
    @SneakyThrows
    public static void main(String[] args) {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);
        File tmpFile = new File(GenTableClass.class.getClassLoader().getResource("template").getFile());
        cfg.setDirectoryForTemplateLoading(tmpFile);
        cfg.setDefaultEncoding("UTF-8");
        Template template = cfg.getTemplate("tableClass.ftl");
        Class.forName("com.cloudera.impala.jdbc41.Driver");
        Connection connection = DriverManager.getConnection("jdbc:impala://slave01:21050/owlx;auth=noSasl;");
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SHOW TABLES");
        ArrayList<String> tableNames = Lists.newArrayList();
        while (rs.next()) {
            String tableName = rs.getString(1);
            //袁浩的这个表有个什么时间戳的类型 坑爹
            if ("t_check_tab".equals(tableName)) {
                continue;
            }
            tableNames.add(tableName);
        }
        rs.close();
        statement.close();
        for (String tableName : tableNames) {
            statement = connection.createStatement();
            rs = statement.executeQuery("select * from " + tableName +" limit 1");
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            TableModel tableModel = new TableModel();
            tableModel.setTableName(CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.UPPER_CAMEL).convert(tableName));
            List<TableModel.MetaData> metaDataList = tableModel.getMetaDataList();
            for (int i = 1; i <= columnCount; i++) {
                String columnTypeName = metaData.getColumnTypeName(i);
                String columnName = metaData.getColumnName(i);
                if ("BIGINT".equals(columnTypeName)) {
                    columnTypeName = "BIG_INT";
                }
                //这个是关键字 O_O
                if ("type".equals(columnName)) {
                    columnName = "`"+ columnName+"`";
                }

                metaDataList.add(new TableModel.MetaData(columnName,
                        CaseFormat.UPPER_UNDERSCORE.converterTo(CaseFormat.UPPER_CAMEL).convert(columnTypeName)));
            }
            tableModel.setPackageName("com.starv.table.owlx");
            template.process(tableModel, new FileWriter("C:\\StarvCode\\mgtvSpark2\\src\\main\\java\\com\\starv\\table\\owlx\\" + tableModel
                    .getTableName() + ".scala"));
            rs.close();
            statement.close();
        }

        connection.close();



    }
}
