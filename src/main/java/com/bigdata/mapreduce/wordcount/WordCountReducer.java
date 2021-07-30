package com.bigdata.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Reduce Task收集到一组相同key的数据，调一次本方法，比如(a，1)(a，1)(a，1)......(a，1)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 每迭代一次，迭代器就会取一对key-value数据：
        // 取到的key数据重新设置给方法中的参数key
        // 取到的value数据则会设置给以下for循环中的临时变量value
        for (IntWritable value : values) {
            // 将本次迭代到的value值累加到sum变量
            sum += value.get();
        }
        // 将这一组数据的聚合结果通过context返回给reduce task
        context.write(key, new IntWritable(sum));
    }

}
