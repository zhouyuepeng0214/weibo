package com.atguigu.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Exer {

    private static Connection connection = null;

    static {


        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop110,hadoop111,hadoop112");
            conf.set("hbase.zookeeper.property.clientPort","2181");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createTable(String tableName,String... families) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : families) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
            desc.addFamily(hColumnDescriptor);
        }
        admin.createTable(desc);

        admin.close();
    }

}
