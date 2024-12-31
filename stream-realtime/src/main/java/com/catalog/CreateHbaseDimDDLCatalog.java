package com.catalog;

import com.stream.common.utils.ConfigUtils;

/**
 * @Author yiqun.shi
 * @Date 2024/12/31 9:54
 * @description: 这个包的整体思路就是把每次都要重复些的建表语句放到catalog里，因为我们要关注的是业务(查询语句)，而不关心怎么建的表，所以将建表语句放到catalog里(存入hive中)。那么这个就是第一个类
 *               本来想建一个包叫做catalog/ddl/hbase的，但是无效名字，所以叫catalog了。
 *               第二个类就是
 *               第三个(工具)类叫HiveCatalogUtils，在"stream-realtime/src/main/java/com/stream/utils/HiveCatalogUtils.java"里
 */
public class CreateHbaseDimDDLCatalog {

    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String ZOOKEEPER_SERVER_HOST_LIST = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String HBASE_CONNECTION_VERSION = "hbase-2.2";
    private static final String DROP_TABEL_PREFIX = "drop table if exists ";
    private static final String createHbaseDimBaseDicDDL = "CREATE TABLE hbase_dim_base_dic (\n" +
            "  rk string,\n" +
            "  info row <dic_name string,parent_code string>,\n" +
            "  primary key (rk) not enforced\n" +
            ") WITH (\n" +
            "  'connector' = '\"+HBASE_CONNECTION_VERSION+\"',\n" +
            "  'table-name' = '\"+HBASE_NAME_SPACE+\":dim_base_dic',\n" +
            "  'zookeeper.quorum' = '\"+ZOOKEEPER_SERVER_HOST_LIST+\"'\n" +
            ")";

    public static void main(String[] args) {

        //

    }

}

/*
CREATE TABLE hbase_dim_base_dic (
  rk string,
  info row <dic_name string,parent_code string>,
  primary key (rk) not enforced
) WITH (
  'connector' = '"+HBASE_CONNECTION_VERSION+"',
  'table-name' = '"+HBASE_NAME_SPACE+":dim_base_dic',
  'zookeeper.quorum' = '"+ZOOKEEPER_SERVER_HOST_LIST+"'
)
 */
