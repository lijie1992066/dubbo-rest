package cn.lijie.hbase;

import cn.lijie.comm.CommUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtils {

    public static void main(String[] args) throws IOException {
        trancate("user_variables_test");

    }

    private static Connection con = null;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop03:2181");
        try {
            con = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void trancate(String tablename) throws IOException {
        TableName tableName = TableName.valueOf(Bytes.toBytes(tablename));
        Admin hBaseAdmin = con.getAdmin();
        try {
            // 判断表是否存在
            if (hBaseAdmin.tableExists(tableName)) {

                hBaseAdmin.disableTable(tableName);

                // 清空表
                hBaseAdmin.truncateTable(tableName, false);

            }
        } catch (Exception e) {
            e.getStackTrace();
        } finally {
            closeConnect(hBaseAdmin);
        }
    }

    public static void delete(String tablename, String row,int flag) throws Exception {

        if(flag == 1){
            row = CommUtils.reverse(row);
        }

        // 获取htable操作对象
        Table table = con.getTable(TableName.valueOf(tablename));

        if (table != null) {
            try {
                // 创建删除对象
                Delete d = new Delete(row.getBytes());
                // 执行删除操作
                table.delete(d);
            } catch (IOException e) {
                throw new RuntimeException("delete单条数据 删除数据失败!" + e.getMessage());
            } finally {
                // 关闭连接
                closeConnect(table);
            }
        }
    }

    /**
     * 关闭 ResultScanner
     */
    public static void closeConnect(ResultScanner scanner) {
        closeConnect(scanner, null, null, null, null);
    }

    /**
     * 关闭 BufferedMutator
     *
     * @param mutator
     */
    public static void closeConnect(BufferedMutator mutator) {
        closeConnect(null, mutator, null, null, null);
    }

    /**
     * 关闭 admin
     *
     * @param admin
     */
    public static void closeConnect(Admin admin) {
        closeConnect(null, null, admin, null, null);
    }

    /**
     * 关闭 htable
     *
     * @param htable
     */
    public static void closeConnect(HTable htable) {
        closeConnect(null, null, null, htable, null);
    }

    /**
     * 关闭 table
     *
     * @param table
     */
    public static void closeConnect(Table table) {
        closeConnect(null, null, null, null, table);
    }

    /**
     * 关闭连接
     *
     * @param mutator
     * @param admin
     * @param htable
     */
    public static void closeConnect(ResultScanner scanner, BufferedMutator mutator, Admin admin,
                                    HTable htable, Table table) {

        if (null != scanner) {
            try {
                scanner.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (null != mutator) {
            try {
                mutator.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (null != admin) {
            try {
                admin.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (null != htable) {
            try {
                htable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (null != table) {
            try {
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
