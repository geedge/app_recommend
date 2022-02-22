package com.galaxy.cn.config;

import java.util.Map;

/**
 * Created by wk on 2021/1/6.
 */
public class commonConfig {


  public static final String SOURCE_KAFKA_BROKER = CommonConfigurations.getStringProperty("source.kafka.broker");
  public static final String SOURCE_KAFKA_GROUP_ID = CommonConfigurations.getStringProperty("source.kafka.group.id");
  public static final String SOURCE_KAFKA_TOPIC = CommonConfigurations.getStringProperty("source.kafka.topic");
  public static final String ZK_HOST = CommonConfigurations.getStringProperty("zk.host");

  public static final int SOURCE_KAFKA_PARALLELISM = CommonConfigurations.getIntProperty("source.kafka.parallelism");
  public static final int SINK_HBASE_PARALLELISM = CommonConfigurations.getIntProperty("sink.hbase.parallelism");
  public static final String SINK_HBASE_FM = CommonConfigurations.getStringProperty("sink.hbase.fm");
  public static final String SINK_HBASE_TABLE = CommonConfigurations.getStringProperty("sink.hbase.table");

  public static final int TASK_PARALLELISM = CommonConfigurations.getIntProperty("task.parallelism");

  public static final int WATERMARK_TIME = CommonConfigurations.getIntProperty("watermark.time");
  public static final int SLIDINGWINDOW_TIME_MINUTE = CommonConfigurations.getIntProperty("slidingwindow.time.minute");
  public static final int SLIDINGWINDOWSLOT_TIME_MINUTE = CommonConfigurations.getIntProperty("slidingwindowslot.time.minute");
  public static final int TOP_LIMIT = CommonConfigurations.getIntProperty("top.limit");

  public static final String KAFKA_USER = CommonConfigurations.getStringProperty("kafka.user");
  public static final String KAFKA_PIN = CommonConfigurations.getStringProperty("kafka.pin");
  public static final int KAFKA_SECURITY = CommonConfigurations.getIntProperty("kafka.security");
  public static final String TOOLS_LIBRARY = CommonConfigurations.getStringProperty("tools.library");

  public static final Boolean no_filter = CommonConfigurations.getBooleanProperty("has.filter");
  public static final Map<String,String> app_white_list= CommonConfigurations.getHashTableProperty("app.white.list");

  public static final String SESSION_TIMEOUT_MS=CommonConfigurations.getStringProperty("session.timeout.ms");
  public static final String MAX_POLL_RECORD=CommonConfigurations.getStringProperty("max.poll.records");
  public static final String MAX_PARTITION_FETCH_BYTES=CommonConfigurations.getStringProperty("max.partition.fetch.bytes");

}
