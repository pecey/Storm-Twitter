package com.pecey.util;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

import java.util.Map;

public class PrintBolt implements IRichBolt {

  OutputCollector _collector;
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    _collector =  outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    Status tweet = (Status) tuple.getValueByField("tweet");
    String lang = tweet.getUser().getName();
    System.out.println("Tweet By :" + lang);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
