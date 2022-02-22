package com.galaxy.cn.sink;

import com.galaxy.cn.config.commonConfig;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.Serializable;

import static cn.hutool.crypto.SecureUtil.md5;

public class HbaseSink extends RichSinkFunction<Tuple3<String,String,Long>> implements Serializable, SinkFunction<Tuple3<String, String,Long>> {
    private Logger log;

    private String hbase_zookeeper_host;

    private Connection connection;
    private Admin admin;

    public HbaseSink(String hbase_zookeeper_host) {
        this.hbase_zookeeper_host = hbase_zookeeper_host;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log = Logger.getLogger(HbaseSink.class);

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_host);

        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    public void invoke(Tuple3<String,String,Long> data, Context context) throws Exception {
        // 按 project:table 归纳


        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(commonConfig.SINK_HBASE_TABLE));
            Put put = new Put(Bytes.toBytes(md5(data.f0)));
            put.addColumn(Bytes.toBytes(commonConfig.SINK_HBASE_FM), Bytes.toBytes("app_label"), Bytes.toBytes(data.f0));
            put.addColumn(Bytes.toBytes(commonConfig.SINK_HBASE_FM), Bytes.toBytes("client_ip_list"), Bytes.toBytes(data.f1));
            put.addColumn(Bytes.toBytes(commonConfig.SINK_HBASE_FM), Bytes.toBytes("last_update_time"), Bytes.toBytes(data.f2));
            table.put(put);
        } catch (Exception e) {
            log.error(e.toString());
        } finally {
            table.close();
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}
