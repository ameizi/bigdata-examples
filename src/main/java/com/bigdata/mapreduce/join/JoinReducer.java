package com.bigdata.mapreduce.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
        List<JoinBean> list = new ArrayList<>();
        JoinBean v = new JoinBean();
        for (JoinBean value : values) {
            try {
                // 数据分类为客户时
                if ("customer.txt".equals(value.getDataType())) {
                    BeanUtils.copyProperties(v, value);
                } else {
                    // 数据分类为订单时，聚合其相关数据
                    JoinBean joinBean = new JoinBean();
                    BeanUtils.copyProperties(joinBean, value);
                    list.add(joinBean);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 遍历订单数据，并为其赋值上客户相关的数据
        for (JoinBean joinBean : list) {
            joinBean.set(v.getCustomerId(), v.getName(), v.getAddress(), v.getPhone(), joinBean.getOrderId(), joinBean.getPrice(), joinBean.getDataType());
            context.write(joinBean, NullWritable.get());
        }
    }

}
