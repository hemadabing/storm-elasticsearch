package com.hemadabing.storm.bolt;

import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;

/**
 * Created by shibingxin on 2018/3/7.
 */
public class ParseBolt extends BaseRichBolt {

  private OutputCollector collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    String source = input.getSourceComponent();
    MessageId messageId = input.getMessageId();
    String streamId = input.getSourceStreamId();
    List<Object> values = input.getValues();
    String sour = new src/main/java/com/hemadabing/storm/bolt/ParseBolt.javasrc/main/java/com/hemadabing/storm/bolt/ParseBolt.javaString((byte[]) values.get(0));
    System.out.println(sour);
    collector.ack(input);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
