package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDim;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;




/**
 * @Package com.retailersv1.DbusCdc2KafkaTopic
 * @Author zhou.han
 * @Date 2024/12/12 12:56
 * @description: mysql db cdc to kafka realtime_db topic
 */
public class DbusCdc2KafkaTopic {

    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        //cdc读数据库
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        //读配置表
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "gmall2024_config.table_process_dim,gmall2024_config.table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");

        /*
        {"before":null,"after:{"id":3452,"consignee":"苗芳燕","consignee_tel":"13957195258","total_amount":"BQis","order_status":"1002","user_id":267,"payment_way":"3501","delivery_address":null,"order_comment":null,"out_trade_no":"685883577882965","trade_body":"小米电视4A 70英寸 4K超高清 HDR 二级能效 2GB+16GB L70M5-4A 内置小爱 智能网络液晶平板教育电视等1件商品","create_time":1733925996000,"operate_time":1733925997000,"expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":17,"activity_reduce_amount":"AA==","coupon_reduce_amount":"AA==","original_total_amount":"BQis","feight_fee":null,"feight_fee_reduce":null,"refundable_time":1734530796000},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024","sequence":null,"table":"order_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1734936384387,"transaction":null}
        */
        //lambda表达式 cdcDbDimStream.map(x->JSONObject.parseObject(x))
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

        /*
        {"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_base_trademark","source_table":"base_trademark","sink_columns":"id,tm_name"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024_config","table":"table_process_dim"},"ts_ms":1734919517472}
         */
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);


        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");

        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");

        //声明,将配置表流整成广播流
        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);

        //主流关联广播流
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);

        //处理关联后的流
        connectDs.process(new ProcessSpiltStreamToHBaseDim(mapStageDesc));


        /*
        运行下面的话就会就会生成top_db并存入数据
         */
//        cdcDbMainStream.sinkTo(
//                KafkaUtils.buildKafkaSink(ConfigUtils.getString("kafka.bootstrap.servers"),"topic_db")
//        ).uid("sink_to_kafka_realtime_v1_mysql_db").name("sink_to_kafka_realtime_v1_mysql_db");

        env.execute();
    }

}
