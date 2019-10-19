package com.atguigu.hbase.weibo.constant;

public class Names {

    public final static String NAMESPACE_WEIBO = "weibo";

    public final static String TABLE_WEIBO = "weibo:weibo";
    public final static String TABLE_RELATION = "weibo:relation";
    public final static String TABLE_INBOX = "weibo:inbox";

    public final static String WEIBO_FAMILY_DATA = "data";
    public final static String RELATION_FAMILY_DATA = "data";
    public final static String INBOX_FAMILY_DATA = "data";

    public final static String WEIBO_COLUMN_CONTENT = "content";
    public final static String RELATION_COLUMN_TIME = "time";

    public final static Integer INBOX_DATA_VERSIONS = 3;

}
