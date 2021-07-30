package com.bigdata.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEY IN : 行的起始偏移量，原生类型long，对应的Hadoop序列化类型：LongWritable
 * VALUE IN:行的文本内容，原生类型String，对应的Hadoop序列化类型：Text
 * KEY OUT: 一个单词，原生类型String，对应的Hadoop序列化类型：Text
 * VALUE OUT: 单词次数1，原生类型int，对应的Hadoop序列化类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * maptask程序在自己的任务范围数据中（数据切片）
     * 每读取一行数据，就调一次本map()方法，并将读到的数据以key-value形式作为参数传给本map()方法
     * map()方法按照所需的数据处理逻辑对传入的数据进行处理，然后通过context返回给map task程序
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key在默认情况下是maptask所读取到的一行文本的起始偏移量
        // value在默认情况下是maptask所读取到的一行文本的字符内容

        // 将文本按空格符号切分成单词数组
        for (String word : value.toString().split(" ")) {
            // 遍历数组中的每一个单词，将每个单词变成  (单词，1)这样的kv对数据，通过context返回给maptask
            context.write(new Text(word), new IntWritable(1));
        }
    }

}
