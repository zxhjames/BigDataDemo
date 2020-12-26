
package cn.itcast.hbase_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class testHbaseApi {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main( String[] args ) throws IOException {
        init();
       // createTable("img",new String[]{"dir"});
      //  insertData("img","imgname","dir",null, "/user/img");
        getData("img", "imgname", "dir",null);
        close();
    }

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://node1:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(){
        try {
            if (admin != null) admin.close();
            if (connection != null) connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tableName, String[] colFamily) throws IOException {
        init();
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)){
            System.out.println("Table exists!!");
        }else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(name);
            for (String str : colFamily){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                tableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDescriptor);
            System.out.println("create success");
        }
        close();
    }

    public static void insertData(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        if(col!=null) {
            put.addColumn(colFamily.getBytes(),col.getBytes(), val.getBytes());
        }else{
            put.addColumn(colFamily.getBytes(),null,val.getBytes());
        }
        table.put(put);
        table.close();
    }

    public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        if (col != null) get.addColumn(colFamily.getBytes(), col.getBytes());
        else get.addFamily(colFamily.getBytes());
        Result result = table.get(get);
        System.out.println(new String(result.getValue(colFamily.getBytes(), col == null ? null : col.getBytes())));
    }

}