package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDwd;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author yiqun.shi
 * @Date 2024/12/26 13:47
 * @description: dwd_db的工具类
 */
public class ProcessDwdFunction extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    //实例化描述器
    private final MapStateDescriptor<String, JSONObject> state;

    private  HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

    public ProcessDwdFunction(MapStateDescriptor<String, JSONObject> state) {
        this.state = state;
    }


    //将配置表转化成实体类
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String querySql = "select * from gmall2024_config.table_process_dwd";
        List<TableProcessDwd> tableProcessDwds = JdbcUtils.queryList(connection, querySql, TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
            hashMap.put(tableProcessDwd.getSourceTable(),tableProcessDwd);
        }
    }

    //

    /*
    主流
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(state);
        JSONObject after = jsonObject.getJSONObject("after");
        String tableName = jsonObject.getJSONObject("source").getString("table");
        if(broadcastState!=null){
            ArrayList<JSONObject> alist = new ArrayList<>();
            if(hashMap.containsKey(tableName)){
                alist.add(after);
                KafkaUtils.sinkJson2KafkaMessage(tableName,alist);
            }
        }
    }

    /*
    广播流
     */
    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(state);
        String tableName = jsonObject.getJSONObject("after").getString("tableName");
        broadcastState.put(tableName,jsonObject);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}
