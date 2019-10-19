package com.atguigu.hbase.weibo.controller;

import com.atguigu.hbase.weibo.service.WeiboService;

import java.io.IOException;
import java.util.List;

public class WeiboController {

    private WeiboService service = new WeiboService();

    public void init () throws IOException {
        service.init();
    }

    //5.发布微博内容
    public void publish (String star,String content) throws IOException {

        service.publish(star,content);
    }

    //6.添加关注用户
    public void follow (String fans,String star) throws IOException {
        service.follow(fans,star);

    }

    //7.取消关注
    public void unFollow (String fans,String star) throws IOException {

        service.unFollow(fans,star);
    }

    //8)获取关注的人的微博内容
    //8.1)获取某个明星的所有微博
    public List<String> getAllWeibosByUserId (String star) throws IOException {
        return service.getAllWeibosByUserId(star);
    }
    //8.2)获取关注的所有star的近期weibo
    public List<String> getAllRecentWeibos (String fans) throws IOException {
        return service.getAllRecentWeibos(fans);
    }


}
