package com.bigdata.mapreduce.commonfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonFriendStep2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text pair, Iterable<Text> friends, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 构造一个StringBuilder用于拼接字符串
        StringBuilder sb = new StringBuilder();
        // 将这个用户对的所有共同好友拼接在一起
        for (Text f : friends) {
            sb.append(f).append(",");
        }
        // 将用户对作为key，拼接好的所有共同好友作为value，返回给reduce task
        context.write(pair, new Text(sb.substring(0, sb.length() - 1)));
    }
}
