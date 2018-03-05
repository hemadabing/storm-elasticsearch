package com.hemadabing.storm.bolt;

import static java.util.Objects.requireNonNull;

import com.hemadabing.storm.common.EsTupleMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Created by shibingxin on 2018/3/5.
 */
public class EsIndexBolt extends AbstractEsBolt{

  private final EsTupleMapper tupleMapper;

  private final ObjectMapper mapper = new ObjectMapper();

  public EsIndexBolt(Config esConfig,EsTupleMapper tupleMapper) {
    super(esConfig);
    this.tupleMapper = requireNonNull(tupleMapper);
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);
  }


  @Override
  public void execute(Tuple tuple) {
    try {
      String source = tupleMapper.getSource(tuple);
      String index = tupleMapper.getIndex(tuple);
      String type = tupleMapper.getType(tuple);
      String id = tupleMapper.getId(tuple);
      Map<String, String> params = tupleMapper.getParams(tuple, new HashMap<>());
      client.prepareIndex("test-es", "type1").setSource(mapper.writeValueAsString(params), XContentType.JSON).get();
      collector.ack(tuple);
    } catch (Exception e) {
      collector.reportError(e);
      collector.fail(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }
}
