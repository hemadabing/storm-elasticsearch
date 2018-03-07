package com.hemadabing.storm.topology;



import com.hemadabing.storm.bolt.ParseBolt;
import com.hemadabing.storm.common.KafkaSpoutConstants;
import com.hemadabing.storm.spout.KafkaSpoutProvider;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by shibingxin on 2018/3/7.
 */
public class ConsumerTopology {


  private static String TOPOLOGY_NAME = "test-consumer-topology";
  private static String BROKERS_ZOOKEEPER = "aliyum1:2181,aliyum2:2181,aliyum3:2181";
  private static String ZOOKEEPER_HOSTS = "aliyum1,aliyum2,aliyum3";
  private static String ZOOKEEPER_PORT = "2181";
  private static String ZOOKEEPER_ROOT = "/test-root";
  private static String GROUPID = "test-group";
  private static String TOPIC ="hdfs_audit_event_sandbox2";

  private static final String SPOUT_ID = "storm-kafka-spout";
  private static final String BOLT_ID = "storm-es-bolt";



  public static void main(String args[])
      throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {


    if (args.length >0 && args.length < 7) {
      System.err.println("Please check command line arguments.");
      System.err.println("Usage :");
      System.err.println(ConsumerTopology.class.toString() + " arguments is error");

      System.err.println(" arguments 1 = topologyName .");
      System.err.println(" arguments 2 = brokers zookeper server  example is s1:2181,s2:2181");
      System.err.println(" arguments 3 = kafka offset zookeeper host ");
      System.err.println(" arguments 4 = kafka offset zookeeper port ");
      System.err.println(" arguments 5 = kafka offset zookeeper root ");
      System.err.println(" arguments 6 = kafka offset groupId ");
      System.err.println(" arguments 7 = kafka topicId ");

      System.err.println();
      System.exit(-1);
    }

    if(args.length >= 7) {
      // 1 - parse cmd line args
      TOPOLOGY_NAME = args[0];
      BROKERS_ZOOKEEPER = args[1];
      ZOOKEEPER_HOSTS = args[2];
      ZOOKEEPER_PORT = args[3];
      ZOOKEEPER_ROOT = args[4];
      if(!ZOOKEEPER_PORT.substring(0,1).equals("/")) {
        ZOOKEEPER_PORT ="/"+ZOOKEEPER_PORT;
      }
      GROUPID = args[5];
      TOPIC = args[6];
    }

    Config config = new Config();
    TopologyBuilder builder=new TopologyBuilder();

    config.put(KafkaSpoutConstants.KAFKA_STORM_TOPOLOGY_NAME,TOPOLOGY_NAME);
    config.put(KafkaSpoutConstants.KAFKA_BROKERS_ZOOKEEPER,BROKERS_ZOOKEEPER);
    config.put(KafkaSpoutConstants.KAFKA_ZOOKEEPER_HOSTS,ZOOKEEPER_HOSTS);
    config.put(KafkaSpoutConstants.KAFKA_ZOOKEEPER_PORT,ZOOKEEPER_PORT);
    config.put(KafkaSpoutConstants.KAFKA_ZOOKEEPER_ROOT,ZOOKEEPER_ROOT);
    config.put(KafkaSpoutConstants.KAFKA_ZOOKEEPER_GROUPID,GROUPID);
    config.put(KafkaSpoutConstants.KAFKA_ZOOKEEPER_TOPIC,TOPIC);

    KafkaSpout kafkaSpout = new KafkaSpoutProvider().getSpout(config);
    builder.setSpout(SPOUT_ID, kafkaSpout,1);
    builder.setBolt(BOLT_ID, new ParseBolt(),1).shuffleGrouping(SPOUT_ID);

    //集群模式
    if(args!=null&&args.length>0){
      config.setDebug(false);
      StormSubmitter.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
    }else{
      //单机模式
      LocalCluster cluster=new LocalCluster();
      cluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
      Utils.sleep(30000l);
      cluster.shutdown();
    }
  }

}
