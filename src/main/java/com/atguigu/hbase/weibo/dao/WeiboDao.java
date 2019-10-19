package com.atguigu.hbase.weibo.dao;

import com.atguigu.hbase.weibo.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiboDao {

    public static Connection connection = null;

    static {

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","hadoop110,hadoop111,hadoop112");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void createNamespace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespce = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(namespce);
        admin.close();
    }


    public void createTable(String tableName, String... families) throws IOException {
        createTable(tableName,1,families);
    }

    public void createTable(String tableName, Integer versions, String... families) throws IOException {
        Admin admin = connection.getAdmin();

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

        for (String family : families) {
            HColumnDescriptor familyDesc = new HColumnDescriptor(Bytes.toBytes(family));
            familyDesc.setMaxVersions(versions);
            tableDesc.addFamily(familyDesc);
        }

        admin.createTable(tableDesc);
        admin.close();
    }

    /**
     * 插入一个列
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));

        table.put(put);
        table.close();
    }


    public List<String> getRowKeysByPrefix(String tableName, String prefix) throws IOException {
        List<String> list = new ArrayList<String>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            byte[] row = result.getRow();
            String rowKey = Bytes.toString(row);
            list.add(rowKey);
        }
        scanner.close();
        table.close();

        return list;
    }

    /**
     *
     * 向多行中相同的列插入相同的value
     * @param tableName
     * @param rowKeys
     * @param family
     * @param column
     * @param value
     */
    public void putCells(String tableName, List<String> rowKeys, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Put> puts = new ArrayList<>();
        for (String rowKey : rowKeys) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
            puts.add(put);
        }

        table.put(puts);
        table.close();

    }

    /**
     * 根据rowKey的范围获取多行的rowKey
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @return
     */
    public List<String> getRowKeysByRange(String tableName, String startRow, String stopRow) throws IOException {

        List<String> list = new ArrayList<>();

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            byte[] row = result.getRow();
            String rowKey = Bytes.toString(row);
            list.add(rowKey);
        }

        scanner.close();
        table.close();

        return list;
    }

    /**
     * 删除一行最新数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void deleteRow(String tableName, String rowKey) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        table.delete(delete);
        table.close();

    }

    /**
     *
     * 删除一行中一列的所有版本
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public void deleteCells(String tableName, String rowKey, String family, String column) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(family),Bytes.toBytes(column));

        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey前缀获取多行中的一列的值
     * @param tableName
     * @param prefix
     * @param family
     * @param column
     * @return
     * @throws IOException
     */
    public List<String> getCellsByPrefix(String tableName, String prefix, String family, String column) throws IOException {

        List<String> list = new ArrayList<>();

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        //前缀过滤器
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
        //指定要获取的列
        scan.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            list.add(Bytes.toString(CellUtil.cloneValue(cells[0])));
        }

        scanner.close();
        table.close();
        return list;
    }


    /**
     * 获取某一行的一个列族的数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     */
    public List<String> getFamilyByRowKey(String tableName, String rowKey, String family) throws IOException {

        List<String> list = new ArrayList<>();

        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        get.setMaxVersions(Names.INBOX_DATA_VERSIONS);

        get.addFamily(Bytes.toBytes(family));

        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            list.add(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        table.close();

        return list;
    }

    /**
     *
     * 获取多行中相同列的数据
     * @param tableName
     * @param rowKeys
     * @param family
     * @param column
     * @return
     */
    public List<String> getCellsByRowKey(String tableName, List<String> rowKeys, String family, String column) throws IOException {

        List<String> weibos = new ArrayList<>();

        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Get> gets = new ArrayList<>();

        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

            gets.add(get);
        }

        Result[] results = table.get(gets);

        for (Result result : results) {
            String weibo = Bytes.toString(CellUtil.cloneValue(result.rawCells()[0]));
            weibos.add(weibo);
        }

        table.close();

        return weibos;
    }
}
