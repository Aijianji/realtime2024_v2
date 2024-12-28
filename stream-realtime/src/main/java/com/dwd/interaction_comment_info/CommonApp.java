package com.dwd.interaction_comment_info;

/**
 * @Author yiqun.shi
 * @Date 2024/12/27 15:07
 * @description:
 */

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 首先要消费业务主题数据(topic_db)
 * 然后筛选出评论数据并且封装为表
 * 再建立hbase表 字典表
 * 字典表和评论表进行关联 退化成了加购表
 * 然后写入kafka评论事实主题
 */
public class CommonApp {



    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        


        /*
        这里不抛异常了
        @SneakyThrows:当一个方法被标注了注解后，Lombok 会在编译时自动将方法中的受检异常包装成一个运行时异常（Runtime Exceptions）并抛出。不需要再throw抛异常和try/catch捕获异常了
         */
        env.execute();
    }
}
