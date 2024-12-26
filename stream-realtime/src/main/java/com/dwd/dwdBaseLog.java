package com.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.ProcessSplitStream;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateFormatUtil;
import com.stream.common.utils.KafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * @Author yiqun.shi
 * @Date 2024/12/24 11:24
 * @description: 写dwdlog的代码
 */
public class dwdBaseLog {

    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_err_log = ConfigUtils.getString("kafka.err.log");
    private static final String kafka_start_log = ConfigUtils.getString("kafka.start.log");
    private static final String kafka_display_log = ConfigUtils.getString("kafka.display.log");
    private static final String kafka_action_log = ConfigUtils.getString("kafka.action.log");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");
    private static final OutputTag<String> errTag = new OutputTag<String>("errTag") {};
    private static final OutputTag<String> startTag = new OutputTag<String>("startTag") {};
    private static final OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
    private static final OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
    private static final HashMap<String, DataStream<String>> collectDsMap = new HashMap<>();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(ConfigUtils.getString("kafka.bootstrap.servers"),
                ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC"),
                "group1",
                OffsetsInitializer.earliest());

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "aaa");

        //输出一下
//        streamSource.print();
        /*
        {"actions":[{"action_id":"favor_add","item":"2","item_type":"sku_id","ts":1734962314853}],"common":{"ar":"26","ba":"vivo","ch":"360","is_new":"1","md":"vivo x90","mid":"mid_279","os":"Android 13.0","sid":"3c7055aa-f418-412a-8efb-67cae0d20f9e","uid":"628","vc":"v2.1.134"},"displays":[{"item":"6","item_type":"sku_id","pos_id":4,"pos_seq":0},{"item":"19","item_type":"sku_id","pos_id":4,"pos_seq":1},{"item":"21","item_type":"sku_id","pos_id":4,"pos_seq":2},{"item":"8","item_type":"sku_id","pos_id":4,"pos_seq":3},{"item":"2","item_type":"sku_id","pos_id":4,"pos_seq":4}],"page":{"during_time":8051,"from_pos_id":8,"from_pos_seq":1,"item":"2","item_type":"sku_id","last_page_id":"home","page_id":"good_detail"},"ts":1734962312853}
         */


        //对流中的数据类型进行转换 并做简单的ETL
        //ETL
        SingleOutputStreamOperator<JSONObject> chulihouDS = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //脏数据 不要了
                }
            }
        });
        //输出看看
//        chulihouDS.print("处理后的数据>>");


        //新老用户校验
        //先按照设备id进行分组
        KeyedStream<JSONObject, String> keyedStream = chulihouDS.keyBy(x -> x.getJSONObject("common").getString("mid"));
        //状态编程 rich前缀的方法才有open()、close()方法。里面会有一个上下文context存储状态信息
        SingleOutputStreamOperator<JSONObject> isNewDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                //从状态中获取首次访问日期
                String lastVisitDate = lastVisitState.value();
                //获取当前访问日期
                Long ts = jsonObject.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);

                if ("1".equals(isNew)) {
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        //如果监控状态为null 认为本次是该访客首次访问，将日志中的ts对应的日期更新到状态中，将日志中的ts对应的日期更新到状态中，不对is_new字段做修改
                        lastVisitState.update(curVisitDate);
                    } else {
                        //如果监控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将is_new字段设置为0
                        if (!lastVisitDate.equals(curVisitDate)) {
                            jsonObject.getJSONObject("common").put("is_new", 0);
                        }
                    }
                } else {
                    //is_new的值为0
                    //如果键控状态为null，说明访问的是老访客但本次是该访客的页面日志首次进去程序。当前端新老访客状态标记丢失时，
                    //日志进入程序被判定为新访客，flink程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日
                    if (StringUtils.isNoneEmpty(lastVisitDate)) {
                        String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVisitState.update(yesterday);
                    }
                }
                return jsonObject;
            }
        });
        //输出一下
//        isNewDS.print();


        //最后分流
        /**
         * 从这往下是粘的老师代码
         */
        SingleOutputStreamOperator<String> processTagDs = isNewDS.process(new ProcessSplitStream(errTag,startTag,displayTag,actionTag))
                .uid("flag_stream_process")
                .name("flag_stream_process");


        SideOutputDataStream<String> sideOutputErrDS = processTagDs.getSideOutput(errTag);
        SideOutputDataStream<String> sideOutputStartDS = processTagDs.getSideOutput(startTag);
        SideOutputDataStream<String> sideOutputDisplayTagDS = processTagDs.getSideOutput(displayTag);
        SideOutputDataStream<String> sideOutputActionTagTagDS = processTagDs.getSideOutput(actionTag);


        //输出瞅一眼
//        sideOutputErrDS.print("错误");
//        sideOutputStartDS.print("启动");
//        sideOutputDisplayTagDS.print("曝光");
//        sideOutputActionTagTagDS.print("行动");
//        processTagDs.print("页面日志");


        collectDsMap.put("errTag",sideOutputErrDS);
        collectDsMap.put("startTag",sideOutputStartDS);
        collectDsMap.put("displayTag",sideOutputDisplayTagDS);
        collectDsMap.put("actionTag",sideOutputActionTagTagDS);
        collectDsMap.put("page",processTagDs);


        SplitDs2kafkaTopicMsg(collectDsMap);


        //这行代码就是为了告诉程序将算子链打散，保证一个算子运行完成后再运行下一个算子
//        env.disableOperatorChaining();

        env.execute();
    }

    public static void SplitDs2kafkaTopicMsg(HashMap<String,DataStream<String>> dataStreamHashMap){

        dataStreamHashMap.get("errTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_err_log))
                .uid("sk_errMsg2Kafka")
                .name("sk_errMsg2Kafka");

        dataStreamHashMap.get("startTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_start_log))
                .uid("sk_startMsg2Kafka")
                .name("sk_startMsg2Kafka");

        dataStreamHashMap.get("displayTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_display_log))
                .uid("sk_displayMsg2Kafka")
                .name("sk_displayMsg2Kafka");

        dataStreamHashMap.get("actionTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_action_log))
                .uid("sk_actionMsg2Kafka")
                .name("sk_actionMsg2Kafka");

        dataStreamHashMap.get("page").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_page_topic))
                .uid("sk_pageMsg2Kafka")
                .name("sk_pageMsg2Kafka");
    }

}
