package com.christy.innerJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/*
 * 相同的programId
 */
public class InnerJoinReducer extends Reducer<Text, Table, Table, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Table> values, Context context) throws IOException, InterruptedException {
        ArrayList<Table> channelBeans = new ArrayList<>();   // 存储频道的集合
        Table programBean = new Table();  // program bean
        for (Table bean : values) {
            if (bean.getFlag().equals("channel")) {
                Table channelBean = new Table();  // 拷贝传递过来的每条数据到集合中
                try {
                    BeanUtils.copyProperties(channelBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                channelBeans.add(channelBean);
            } else {
                try {
                    BeanUtils.copyProperties(programBean, bean);  // 拷贝传递过来的每条数据到内存中
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (Table bean : channelBeans) {  // 表的拼接
            bean.setProgramName(programBean.getProgramName());
            context.write(bean, NullWritable.get());
        }
    }
}
