package com.hemadabing.storm.spout;

import com.hemadabing.storm.common.KafkaSpoutConstants;
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.Config;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.utils.Utils;


public class KafkaSpoutProvider implements StormSpoutProvider {
//  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutProvider.class);
  public KafkaSpoutProvider() {
  }

  @Override
  public KafkaSpout getSpout(Config config) {

    String brokers =  Utils.getString(config.get(KafkaSpoutConstants.KAFKA_BROKERS_ZOOKEEPER));
    BrokerHosts hosts = new ZkHosts(brokers);
    SpoutConfig spoutConfig = new SpoutConfig(hosts,
        Utils.getString(config.get(KafkaSpoutConstants.KAFKA_ZOOKEEPER_TOPIC)),
        Utils.getString(config.get(KafkaSpoutConstants.KAFKA_ZOOKEEPER_ROOT)),
        Utils.getString(config.get(KafkaSpoutConstants.KAFKA_ZOOKEEPER_GROUPID)));

    spoutConfig.zkPort = Utils.getInt(config.get(KafkaSpoutConstants.KAFKA_ZOOKEEPER_PORT));
    String zkHosts = Utils.getString(config.get(KafkaSpoutConstants.KAFKA_ZOOKEEPER_HOSTS));
    String []zKHostArray = zkHosts.trim().split(KafkaSpoutConstants.REGEX);
    List<String> servers = new ArrayList<String>();
    for(String host:zKHostArray) {
      servers.add(host);
    }
    spoutConfig.zkServers = servers;

    return new KafkaSpout(spoutConfig);
  }
}
