package com.dwd;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author yiqun.shi
 * @Date 2024/12/25 16:15
 * @description: 这个是dwd层业务数据
 */
public class dwdBaseDb {

    private static final String kafka_topic_db = ConfigUtils.getString("kafka.topic.db");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    public static void main(String[] args) throws Exception {

        // TODO: 2024/12/25 这个方法是输出参数 输出的东西如果显示的是 ${xxxx} 这种就说明没获取到参数，方便找错了 (commonUtils里要有printCheckPropEnv方法)
        CommonUtils.printCheckPropEnv(
                false,
                kafka_topic_db,
                kafka_botstrap_servers
        );

        //流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //cdc获取mysql主表数据
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        //配置表数据
        MySqlSource<String> mySQLDbConfigCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "gmall2024_config.table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        //获取mysql数据还有配置表数据
        DataStreamSource<String> aa = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "xxx");
        DataStreamSource<String> bb = env.fromSource(mySQLDbConfigCdcSource, WatermarkStrategy.noWatermarks(), "yyy");
//        aa.print();
//        bb.print();

        //数据清洗 脏数据就不要了
        //业务数据应该是没有脏数据
//        SingleOutputStreamOperator<JSONObject> processDS = aa.process(new ProcessFunction<String, JSONObject>() {
//            @Override
//            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
//                try {
//                    out.collect(new JSONObject(s.isEmpty()));
//                } catch (Exception e) {
//                    //脏数据不要了
//                }
//            }
//        });

        //转换格式
        /*
        {"op":"r","after":{"create_time":1639440000000,"attr_name":"硬盘","sku_id":14,"id":55,"value_id":84,"value_name":"512GB","attr_id":66},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024","table":"sku_attr_value"},"ts_ms":1735131642646}
         */
        SingleOutputStreamOperator<JSONObject> cdcDbMainStream = aa.map(x -> JSONObject.parseObject(x))
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

        /*
        {"op":"r","after":{"source_type":"insert","sink_table":"dwd_interaction_favor_add","source_table":"favor_info","sink_columns":"id,user_id,sku_id,create_time"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024_config","table":"table_process_dwd"},"ts_ms":1735131740143}
         */
        SingleOutputStreamOperator<JSONObject> cdcDbDwdStream = bb.map(x -> JSONObject.parseObject(x))
                .uid("dwd_data_convert_json")
                .name("dwd_data_convert_json")
                .setParallelism(1);


        




        env.execute();

    }
}
