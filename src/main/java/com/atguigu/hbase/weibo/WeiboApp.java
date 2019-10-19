package com.atguigu.hbase.weibo;

import com.atguigu.hbase.weibo.controller.WeiboController;

import java.io.IOException;
import java.util.List;

public class WeiboApp {

    private static WeiboController controller = new WeiboController();

    public static void main(String[] args) throws IOException {

//    controller.init();

//        controller.publish("1001","hello world 1");
//        controller.publish("1001","hello world 2");
//        controller.publish("1001","hello world 3");
//        controller.publish("1001","hello world 4");
//        controller.publish("1001","hello world 5");

//        controller.follow("1002","1001");
//        controller.follow("1003","1001");

//        List<String> allRecentWeibos = controller.getAllRecentWeibos("1002");
//
//        for (String allRecentWeibo : allRecentWeibos) {
//            System.out.println(allRecentWeibo);
//        }

//        controller.unFollow("1002","1001");

        List<String> allWeibosByUserId = controller.getAllWeibosByUserId("1001");

        for (String s : allWeibosByUserId) {
            System.out.println(s);
        }


    }
}
