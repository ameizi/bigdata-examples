package com.bigdata.mapreduce.commonfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;


public class CommonFriendStep1Reducer extends Reducer<Text, Text, Text, Text> {

    // f:某个"好友"
    // users:拥有这个f好友的一堆用户
    @Override
    protected void reduce(Text f, Iterable<Text> users, Context context) throws IOException, InterruptedException {

        ArrayList<Text> userList = new ArrayList<>();
        // 将这一组拥有共同好友f的user们从迭代器中取出，放入一个arraylist暂存
        for (Text u : users) {
            userList.add(new Text(u));
        }

        // 对users排个序，以免拼俩俩对时出现A-F 又有F-A的现象
        Collections.sort(userList);

        // 把这一对user进行两两组合，并将:
        //1.组合作为key
        //2.共同的好友f作为value
        //返回给reduce task作为本job的最终结果
        for (int i = 0; i < userList.size() - 1; i++) {
            for (int j = i + 1; j < userList.size(); j++) {
                // 输出 "用户-用户" 两两对，及他俩的共同好友
                context.write(new Text(userList.get(i) + "-" + userList.get(j)), f);
            }
        }
    }

}
