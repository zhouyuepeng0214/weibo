package com.atguigu.hbase.weibo.service;

import com.atguigu.hbase.weibo.constant.Names;
import com.atguigu.hbase.weibo.dao.WeiboDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiboService {

    private WeiboDao dao = new WeiboDao();

    public void init () throws IOException {
        //1.创建命名空间及表名的定义
        dao.createNamespace(Names.NAMESPACE_WEIBO);
        //2.创建微博内容表
        dao.createTable(Names.TABLE_WEIBO,Names.WEIBO_FAMILY_DATA);
        //3.创建用户关系表
        dao.createTable(Names.TABLE_RELATION,Names.RELATION_FAMILY_DATA);
        //4.创建用户微博内容接收邮件表
        dao.createTable(Names.TABLE_INBOX,Names.INBOX_DATA_VERSIONS,Names.INBOX_FAMILY_DATA);

    }

    public void publish(String star,String content) throws IOException {

        //1.在weibo表中插入一条数据
        //rowKey以userId和时间戳拼接而成
        String rowKey =  star + "_" + System.currentTimeMillis();
        dao.putCell(Names.TABLE_WEIBO,rowKey,Names.WEIBO_FAMILY_DATA,Names.WEIBO_COLUMN_CONTENT,content);

        //2.从relation表中获取star的所有fansId
        //根据前缀获取star的所有fans
        String prefix = star + ":followedby:";
        List<String> list = dao.getRowKeysByPrefix(Names.TABLE_RELATION,prefix);

        //如果star没有fans，直接返回
        if (list.size() <= 0) {
            return;
        }

        List<String> fansIds = new ArrayList<>();
        //切分rowKey获取fansId
        for (String row : list) {
            String[] split = row.split(":");
            fansIds.add(split[2]);
        }
        //3.向所有fans的inbox中插入本条weibo的id
        dao.putCells(Names.TABLE_INBOX,fansIds,Names.INBOX_FAMILY_DATA,star,rowKey);

    }

    public void follow(String fans, String star) throws IOException {

        //1.向relation表中插入两条数据
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        String time = System.currentTimeMillis() + "";
        dao.putCell(Names.TABLE_RELATION, rowKey1, Names.RELATION_FAMILY_DATA, Names.RELATION_COLUMN_TIME, time);
        dao.putCell(Names.TABLE_RELATION, rowKey2, Names.RELATION_FAMILY_DATA, Names.RELATION_COLUMN_TIME, time);


        //2.从weibo表中获取star的近期weiboId
        String startRow = star;
        String stopRow = star + "_|";
        List<String> list = dao.getRowKeysByRange(Names.TABLE_WEIBO, startRow, stopRow);

        //如果该star未发布weibo，直接返回
        if (list.size() <= 0) {
            return;
        }

        //如果star微博数量不足3条，则返回所有微博
        int fromIndex = list.size() > Names.INBOX_DATA_VERSIONS ? list.size() - Names.INBOX_DATA_VERSIONS : 0;
        List<String> recentWeiboIds = list.subList(fromIndex, list.size());

        //3.向fans的inbox表中插入star的近期weiboId
        for (String recentWeiboId : recentWeiboIds) {
            dao.putCell(Names.TABLE_INBOX, fans, Names.INBOX_FAMILY_DATA, star, recentWeiboId);
        }
    }

    public void unFollow(String fans, String star) throws IOException {

        //1.删除relation表中的两条数据
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        dao.deleteRow(Names.TABLE_RELATION,rowKey1);
        dao.deleteRow(Names.TABLE_RELATION, rowKey2);

        //2.删除inbox表中的一列

        dao.deleteCells(Names.TABLE_INBOX,fans,Names.INBOX_FAMILY_DATA,star);
    }

    public List<String> getAllWeibosByUserId(String star) throws IOException {
        return dao.getCellsByPrefix(Names.TABLE_WEIBO,star,Names.WEIBO_FAMILY_DATA,Names.WEIBO_COLUMN_CONTENT);
    }

    public List<String> getAllRecentWeibos(String fans) throws IOException {

        //1.从inbox中获取fans的所有的star的近期weiboId
        List<String> list = dao.getFamilyByRowKey(Names.TABLE_INBOX, fans, Names.INBOX_FAMILY_DATA);

        //2.根据weiboID去weibo表中查询内容
        return dao.getCellsByRowKey(Names.TABLE_WEIBO, list, Names.WEIBO_FAMILY_DATA, Names.WEIBO_COLUMN_CONTENT);

    }
}
