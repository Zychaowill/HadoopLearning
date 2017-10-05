package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by yachao on 17/9/16.
 */
public class WriteToHBase {

    private static final Logger log = Logger.getLogger(WriteToHBase.class);
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void createTable(String tableName, String[] family) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor descriptor = new HTableDescriptor(tableName);

        for (int i = 0; i < family.length; i++) {
            descriptor.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            log.error("table exists");
            System.exit(2);
        } else {
            admin.createTable(descriptor);
            log.info("Create table successfully!");
        }
    }

    /**
     * 为表添加数据（适合知道有多少列族的固定表）
     * @param rowKey
     * @param tableName
     * @param column1 第一个列族列表
     * @param value1 第一个列的值的列表
     * @param column2 第二个列族列表
     * @param value2 第二个列的值的列表
     * @throws IOException
     */
    public static void addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2, String[] value2) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));

        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString();
            if (familyName.equals("article")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("author")) {
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        log.info("Add data successfully!");
    }

    /**
     * 根据rowkey查询
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static Result getResult(String tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));

        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            log.info("family: " + Bytes.toString(cell.getFamilyArray()));
            log.info("qualifier: " + Bytes.toString(cell.getQualifierArray()));
            log.info("value: " + Bytes.toString(cell.getValueArray()));
            log.info("Timestamp: " + cell.getTimestamp());
            log.info("-------------------------------------------");
        }

        return result;
    }

    /**
     * 遍历查询hbase表
     * @param tableName
     * @throws IOException
     */
    public static void getResultScan(String tableName, String start_rowkey, String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));

        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));

        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    log.info("row: " + Bytes.toString(cell.getRowArray()));
                    log.info("family: " + Bytes.toString(cell.getFamilyArray()));
                    log.info("qualifier: " + Bytes.toString(cell.getQualifierArray()));
                    log.info("value: " + Bytes.toString(cell.getValueArray()));
                    log.info("timestamp: " + cell.getTimestamp());
                    log.info("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * 查询表中的某一列
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @throws IOException
     */
    public static void getResultByColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));

        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            log.info("family: " + Bytes.toString(cell.getFamilyArray()));
            log.info("qualifier: " + Bytes.toString(cell.getQualifierArray()));
            log.info("value: " + Bytes.toString(cell.getValueArray()));
            log.info("timestamp: " + cell.getTimestamp());
            log.info("-------------------------------------------");
        }
    }

    /**
     * 更新表中某一列
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     * @throws IOException
     */
    public static void updataTable(String tableName, String rowKey, String familyName, String columnName, String value) throws
            IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));

        table.put(put);
        log.info("update table successfully!");
    }

    public static void getResultByVersion(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);

        Result r = table.get(get);
        for (Cell cell : r.listCells()) {
            log.info("family: " + Bytes.toString(cell.getFamilyArray()));
            log.info("qualifier: " + Bytes.toString(cell.getQualifierArray()));
            log.info("value: " + Bytes.toString(cell.getValueArray()));
            log.info("timestamp: " + cell.getTimestamp());
            log.info("-------------------------------------------");
        }
    }

    /**
     * 删除指定的列
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @throws IOException
     */
    public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws
            IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));

        table.delete(deleteColumn);
        log.info(familyName + ":" + columnName + " is deleted successfully!");
    }

    /**
     * 删除指定列
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteAllColumn(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);

        log.info("All columns are deleted successfully!");
    }

    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

        log.info(tableName + " is deleted successfully!");
    }

    public static void main(String[] args) throws IOException {
        String tableName = "blog";
        String[] family = {"article", "author"};

//        createTable(tableName, family);

        String[] column1 = {"title", "content", "tag"};
        String[] value1 = {
                "Head First HBase",
                "HBase is the hadoop database. Use it when you need random, realtime read/write access to your Big data.",
                "Hadoop, HBase, NoSQL"
        };
        String[] column2 = {"name", "nickname"};
        String[] value2 = {"jang", "deerlet"};

        addData("rk4", tableName,column1, value1, column2, value2);
        addData("rk5", tableName,column1, value1, column2, value2);
        addData("rk6", tableName,column1, value1, column2, value2);

        getResultScan(tableName, "rk2", "rk6");
    }
}
