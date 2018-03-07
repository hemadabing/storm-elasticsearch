package com.hemadabing.storm.topology;

import com.hemadabing.storm.bolt.EsIndexBolt;
import com.hemadabing.storm.common.DefaultEsTupleMapper;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Created by shibingxin on 2018/3/5.
 */
public class EsTopo {

  private static Config config = null;
  private static final String SPOUT_ID = "spout";
  private static final String BOLT_ID = "bolt";
  private static final String TOPOLOGY_NAME = "elasticsearch-test-topology2";


  public static void main(String args[])
      throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

    config = new Config();
    config.put("cluster.name", "els");
    config.put("cluster.nodes", "192.168.1.221:9300");

    TopologyBuilder builder = new TopologyBuilder();
    UserDataSpout spout = new UserDataSpout();
    builder.setSpout(SPOUT_ID, spout);
    builder.setBolt(BOLT_ID, new EsIndexBolt(config, new DefaultEsTupleMapper())).globalGrouping(SPOUT_ID);
//    if (args != null && args.length > 0) {
//      config.setNumWorkers(3);
//      StormSubmitter.submitTopology("name", config, builder.createTopology());
//    } else {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    Utils.sleep(30000);
    cluster.killTopology(TOPOLOGY_NAME);
    cluster.shutdown();
//    }
  }

  public static class UserDataSpout extends BaseRichSpout {

    private ConcurrentHashMap<String, Values> pending;
    private SpoutOutputCollector collector;
    private String[] sources = {
        "{\"user\":\"user1\"}",
        "{\"user\":\"user2\"}",
        "{\"user\":\"user3\"}",
        "{\"user\":\"user4\"}"
    };
    private int index = 0;
    private int count = 0;
    private long total = 0L;
    private String indexName = "index1";
    private String typeName = "type1";

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("source", "index", "type", "id"));
    }

    public void open(Map config, TopologyContext context,
        SpoutOutputCollector collector) {
      this.collector = collector;
      this.pending = new ConcurrentHashMap<String, Values>();
    }

    public void nextTuple() {
      String source = sources[index];
      String msgId = UUID.randomUUID().toString();
      Values values = new Values(source, indexName, typeName, msgId);
      this.pending.put(msgId, values);
      this.collector.emit(values, msgId);
      index++;
      if (index >= sources.length) {
        index = 0;
      }
      count++;
      total++;
      if (count > 1000) {
        count = 0;
        System.out.println("Pending count: " + this.pending.size() + ", total: " + this.total);
      }
      Thread.yield();
    }

    public void ack(Object msgId) {
      this.pending.remove(msgId);
    }

    public void fail(Object msgId) {
      System.out.println("**** RESENDING FAILED TUPLE");
      this.collector.emit(this.pending.get(msgId), msgId);
    }
  }
}
