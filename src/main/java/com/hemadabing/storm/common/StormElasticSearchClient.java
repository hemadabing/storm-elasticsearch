package com.hemadabing.storm.common;

import java.io.Serializable;
import java.net.InetAddress;
import org.apache.storm.Config;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import static java.util.Objects.requireNonNull;
/**
 * Created by shibingxin on 2018/3/5.
 */
public class StormElasticSearchClient implements Serializable {

  private final Config config;

  private static final String REGEX =",";
  private static final String COLON =":";


  public StormElasticSearchClient( Config config) {
    this.config = config;
  }
  public TransportClient construct() {
    requireNonNull(config);
    String clusterName =(String)config.get("cluster.name");
    requireNonNull(clusterName);
    String clusterNodes =(String)config.get("cluster.nodes");
    requireNonNull(clusterNodes);
    String []nodes = clusterNodes.trim().split(REGEX);

    Settings esSettings = Settings.builder().put("cluster.name", clusterName) .build();
    TransportClient client = new PreBuiltTransportClient(esSettings);
    for(String node : nodes) {
      String []address = node.split(COLON);
      String host = address[0];
      Integer port =Integer.parseInt(address[1]);
      try {
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));

      }catch (Exception e){

      }
    }
    return client;
  }
}
