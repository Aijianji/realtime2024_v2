package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.HbaseUtils;
import com.stream.common.utils.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.retailersv1.func.ProcessSpiltStreamToHBaseDim
 * @Author zhou.han
 * @Date 2024/12/19 22:55
 * @description:
 */
public class ProcessSpiltStreamToHBaseDim extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    private MapStateDescriptor<String,JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap =  new HashMap<>();
    private final String querySQL = "select * from gmall2024_config.table_process_dim";

    private org.apache.hadoop.hbase.client.Connection hbaseConnection;

    private HbaseUtils hbaseUtils;


    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims ){
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        connection.close();

        /**
         * 这块要是不加是连不上hbase的 会报空指针错误的
         */
        hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseConnection = hbaseUtils.getConnection();
    }

    public ProcessSpiltStreamToHBaseDim(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }


    /*
    主流
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //获取状态对象
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //也是 不用转换成JSONObject对象了 已经是了
        String tableName = jsonObject.getJSONObject("source").getString("table");
        JSONObject broadData = broadcastState.get(tableName);
        //这里可能为null NullPointerException
        if(broadData != null || configMap.get(tableName) != null){
            if(configMap.get(tableName).getSourceTable().equals(tableName)){
                if(!jsonObject.getString("op").equals("d")){
                    JSONObject after = jsonObject.getJSONObject("after");
                    String sinkTableName = configMap.get(tableName).getSinkTable();
                    sinkTableName = "realtime_v2:" + sinkTableName;
                    String hbaseRowKey = after.getString(configMap.get(tableName).getSinkRowKey());
                    Table hbaseConnectionTable = hbaseConnection.getTable(TableName.valueOf(sinkTableName));
                    Put put = new Put(Bytes.toBytes(hbaseRowKey));
                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(String.valueOf(entry.getValue())));
                    }
                    hbaseConnectionTable.put(put);
                    //输出错的
                    System.err.println("put ->"+put.toJSON()+" "+ Arrays.toString(put.getRow()));
                }
            }
        }
    }

    /*
    广播流
     */
    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //获取状态对象
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //不用转换成JSONObject对象了，已经是了
        String op = jsonObject.getString("op");
        if(jsonObject.containsKey("after")){
            String sourceTable = jsonObject.getJSONObject("after").getString("source_table");
            if("d".equals(op)){
                broadcastState.remove(sourceTable);
            }else{
                broadcastState.put(sourceTable,jsonObject);
            }
        }
    }

    /*
    @Override 重写父类的方法
     */
    @Override
    public void close() throws Exception {
        super.close();
        hbaseConnection.close();
    }

}
