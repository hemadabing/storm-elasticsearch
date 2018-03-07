package com.hemadabing.storm.common;

import org.apache.storm.Config;
import org.apache.storm.validation.ConfigValidationAnnotations.isString;

/**
 * Created by shibingxin on 2018/3/7.
 */
public class KafkaSpoutConstants extends Config{

  @isString
  public static final String KAFKA_STORM_TOPOLOGY_NAME = "kafka.storm.topology.name";

  @isString
  public static final String KAFKA_BROKERS_ZOOKEEPER = "kafka.brokers";

  @isString
  public static final String KAFKA_ZOOKEEPER_HOSTS = "kafka.zookeeper.host";

  @isString
  public static final String KAFKA_ZOOKEEPER_PORT = "kafka.zookeeper.port";

  @isString
  public static final String KAFKA_ZOOKEEPER_ROOT = "kafka.zookeeper.root";

  @isString
  public static final String KAFKA_ZOOKEEPER_GROUPID = "kafka.zookeeper.groupId";

  @isString
  public static final String KAFKA_ZOOKEEPER_TOPIC = "kafka.zookeeper.topic";


  public static final String REGEX =",";

}
