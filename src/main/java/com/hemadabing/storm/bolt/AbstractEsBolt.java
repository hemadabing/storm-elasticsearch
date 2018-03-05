package com.hemadabing.storm.bolt;

import com.hemadabing.storm.common.StormElasticSearchClient;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shibingxin on 2018/3/5.
 */
public class AbstractEsBolt extends BaseRichBolt{



  private static final Logger LOG = LoggerFactory.getLogger(AbstractEsBolt.class);

  protected Config config;

  protected TransportClient client;
  protected OutputCollector collector;

  public AbstractEsBolt(Config config) {
    this.config = config;
  }


  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    synchronized (AbstractEsBolt.class) {
      if (client == null) {
          client = new StormElasticSearchClient(config).construct();
      }
    }
  }

  @Override
  public void execute(Tuple input) {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }


}
