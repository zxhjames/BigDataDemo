package cn.itcast.hbase_api;

import jdk.nashorn.internal.runtime.logging.DebugLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/****
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * DML
 * 5.插入数据
 * 6.查数据(get)
 * 7.查数据(scan)
 * 8.改数据
 * 9.删除数据
 */
public class TestApi {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static Table table;

    public static void main(String[] args) throws IOException {
        Init();
        createTable("Home",new String[]{"member","loc"});
        close();
    }
    public static void Init() throws IOException {
        configuration = HBaseConfiguration.create();
        //zookeeper地址
        configuration.set("hbase.zookeeper.quorum","192.168.59.128");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("dept"));
        admin = connection.getAdmin();
    }
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
            if (table!=null){
                table.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /****
     * 创建元数据表
     * @param tabName
     * @param colFamily
     * @throws IOException
     */
    public static void createTable(String tabName,String []colFamily) throws IOException {
        TableName tableName = TableName.valueOf(tabName);
        if(admin.tableExists(tableName)){
            System.out.println("表已经存在");
        }else{
            TableDescriptorBuilder tableDescriptorBuilder =
                    TableDescriptorBuilder.newBuilder(tableName);
            Arrays.stream(colFamily).forEach(s ->
                    tableDescriptorBuilder.setColumnFamily(
                            ColumnFamilyDescriptorBuilder.of(s)
                    )
            );
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表创建成功!");
        }
    }


    public static void DropTable(String tabName) throws IOException {
        TableName tableName = TableName.valueOf(tabName);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println("删除表成功");
        close();
    }
}
