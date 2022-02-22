package com.galaxy.recommend;

import com.alibaba.fastjson.JSON;
import com.galaxy.cn.common.Entity;
import com.galaxy.cn.common.ObjectEntity;
import com.galaxy.cn.config.commonConfig;
import com.galaxy.cn.function.TopNHotItems;
import com.galaxy.cn.function.metricsCalculate;
import com.galaxy.cn.sink.HbaseSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Recommendation {
    private static final Logger LOG = LoggerFactory.getLogger(Recommendation.class);

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定使用事件时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String topic = parameterTool.get("topic", commonConfig.SOURCE_KAFKA_TOPIC);
        Properties properties = parameterTool.getProperties();
        properties.setProperty("group.id", commonConfig.SOURCE_KAFKA_GROUP_ID);
        properties.setProperty("bootstrap.servers", commonConfig.SOURCE_KAFKA_BROKER);
        properties.setProperty("session.timeout.ms", commonConfig.SESSION_TIMEOUT_MS);
        properties.setProperty("max.poll.records", commonConfig.MAX_POLL_RECORD);
        properties.setProperty("max.partition.fetch.bytes", commonConfig.MAX_PARTITION_FETCH_BYTES);

        switch (commonConfig.KAFKA_SECURITY) {
            case 1:
                properties.put("security.protocol", "SSL");
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                properties.put("ssl.keystore.location", commonConfig.TOOLS_LIBRARY + "keystore.jks");
                properties.put("ssl.keystore.password", commonConfig.KAFKA_PIN);
                properties.put("ssl.truststore.location", commonConfig.TOOLS_LIBRARY + "truststore.jks");
                properties.put("ssl.truststore.password", commonConfig.KAFKA_PIN);
                properties.put("ssl.key.password", commonConfig.KAFKA_PIN);
                break;
            case 2:
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                        + commonConfig.KAFKA_USER + " password=" + commonConfig.KAFKA_PIN + ";");
                break;
            default:
        }
        DataStream<String> source = env.addSource(new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(), properties)).setParallelism(commonConfig.SOURCE_KAFKA_PARALLELISM);
        WatermarkStrategy<Entity> strategy = WatermarkStrategy
                .<Entity>forBoundedOutOfOrderness(Duration.ofSeconds(commonConfig.WATERMARK_TIME))
                .withTimestampAssigner((Entity, timestamp) -> Entity.getCommon_recv_time()*1000);


        SingleOutputStreamOperator<Entity>  input = source.map(new MapFunction<String, Entity>() {
            @Override
            public Entity map(String message)  {
                Entity entity =new Entity();
                try {
                    entity = JSON.parseObject(message, Entity.class);


                    if(!commonConfig.no_filter){

                        if(entity.getCommon_app_label()==null ||"".equals(entity.getCommon_app_label().trim())){
                            entity.setIfError(1);
                        }
                    }
                    else{
                        if(!commonConfig.app_white_list.containsKey(entity.getCommon_app_label())){
                            entity.setIfError(1);
                        }
                    }
                }
                catch (Exception e){
                    LOG.error("Entity Parsing ERROR");
                    entity.setIfError(1);
                }
                return entity;
            }
        }).filter(new FilterFunction<Entity>() {
            @Override
            public boolean filter(Entity entity) throws Exception {

                return entity.ifError!=1;
            }
        });



        KeyedStream<Entity, Tuple2<String,String>> keyedStream = input.assignTimestampsAndWatermarks(strategy).keyBy(new MyKeySelector());

        SingleOutputStreamOperator<ObjectEntity> windowedStream = keyedStream
                .window(SlidingEventTimeWindows.of(Time.minutes(commonConfig.SLIDINGWINDOW_TIME_MINUTE), Time.minutes(commonConfig.SLIDINGWINDOWSLOT_TIME_MINUTE)))
                .process(new metricsCalculate());

        SingleOutputStreamOperator<Tuple3<String,String,Long>> windoweddStream = windowedStream.keyBy(new oneKeySelector())
                .process(new TopNHotItems(commonConfig.TOP_LIMIT));
        windoweddStream.addSink(new HbaseSink(commonConfig.ZK_HOST)).setParallelism(commonConfig.SINK_HBASE_PARALLELISM);
        env.setParallelism(commonConfig.TASK_PARALLELISM);
        env.execute("RECOMMENDATION-APP-CIP");

    }
    public static class MyKeySelector implements KeySelector<Entity,Tuple2<String,String>> {

        @Override
        public Tuple2<String,String> getKey(Entity entity) throws Exception {
            return new Tuple2<>(entity.getCommon_app_label(),entity.getCommon_client_ip());
        }
    }
    public static class oneKeySelector implements KeySelector<ObjectEntity,Tuple1<String>> {

        @Override
        public Tuple1<String> getKey(ObjectEntity entity) throws Exception {
            return new Tuple1<>(entity.getCommon_app_label());
        }
    }


}