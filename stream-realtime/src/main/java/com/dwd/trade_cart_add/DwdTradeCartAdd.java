package com.dwd.trade_cart_add;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Author yiqun.shi
 * @Date 2024/12/28 10:51
 * @description: 交易域加购事务事实表
 */
public class DwdTradeCartAdd {

    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_topic_db = ConfigUtils.getString("kafka.topic.db");
    private static final String kafka_topic_dwd_trade_cart_add = ConfigUtils.getString("kafka.topic.dwd.trade.cart.add");

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1); 不这么写啦
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> streamSource = env.fromSource(KafkaUtils.buildKafkaSource(
                kafka_bootstrap_servers,
                kafka_topic_db,
                "test01",
                OffsetsInitializer.earliest()
        ), WatermarkStrategy.noWatermarks(), "kafka_topic_db");

        //转换一下格式
        SingleOutputStreamOperator<JSONObject> kafkaSource_topic_db = streamSource.map(x -> JSON.parseObject(x))
                .uid("kafkaSource_topic_db")
                .name("kafkaSource_topic_db");
        //输出瞅一眼
//        kafkaSource_topic_db.print();

        SingleOutputStreamOperator<JSONObject> mapDS = kafkaSource_topic_db.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
            @Override
            public void flatMap(JSONObject jsonObject, Collector<JSONObject> out) throws Exception {
                if (jsonObject.getJSONObject("source").getString("table").equals("cart_info")) {
                    out.collect(jsonObject);
                }
            }
        });
        //输出瞅一眼
//        mapDS.print();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE topic_db (\n" +
                "op string," +
                "db string," +
                "before map<String,String>," +
                "after map<String,String>," +
                "source map<String,String>," +
                "ts_ms bigint," +
                "row_time as TO_TIMESTAMP_LTZ(ts_ms,3)," +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + kafka_topic_db + "',\n" +
                "  'properties.bootstrap.servers' = '" + kafka_bootstrap_servers + "',\n" +
                "  'properties.group.id' = 'test01',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        //beyond输出瞅一眼
        tenv.sqlQuery("select * from topic_db");//.execute().print();

        Table table = tenv.sqlQuery("select " +
                "`after` ['id'] as id,\n" +
                "`after` ['user_id'] as user_id,\n" +
                "`after` ['sku_id'] as sku_id,\n" +
                "`after` ['cart_price'] as cart_price,\n" +
                "if(op='c',cast(after['sku_num'] as bigint),cast(after['sku_num'] as bigint)-cast(before['sku_num'] as bigint)) sku_num,\n" +
                "`after` ['img_url'] as img_url,\n" +
                "`after` ['sku_name'] as sku_name,\n" +
                "`after` ['is_checked'] as is_checked,\n" +
                "`after` ['create_time'] as create_time,\n" +
                "`after` ['operate_time'] as operate_time ,\n" +
                "`after` ['is_ordered'] as is_ordered,\n" +
                "`after` ['order_time'] as order_time," +
                "ts_ms as ts_ms " +
                "from topic_db " +
                "where source['table']='cart_info' and source['db']='gmall2024' " +
                "and (op='r' or (op='u' and before['sku_num'] is not null " +
                "and cast (after['sku_num'] as bigint) > cast(before['sku_num'] as bigint)))");

        //瞅瞅怎么事
//        table.execute().print();

        DataStream<Row> rowDataStream = tenv.toDataStream(table);
//        SingleOutputStreamOperator<String> map = rowDataStream.map(String::valueOf); 这种写法叫做"方法引用"
        //这种是"lambda表达式"
        SingleOutputStreamOperator<String> map = rowDataStream.map(x -> String.valueOf(x));
        //输出
        map.print();
//        map.sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers,kafka_topic_dwd_trade_cart_add));



        env.execute();
    }
}
