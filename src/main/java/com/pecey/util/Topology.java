package com.pecey.util;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.util.Properties;

public class Topology {
  public static void main(String[] args) throws InterruptedException, IOException {
    TopologyBuilder builder = new TopologyBuilder();
    String spoutName = "twitterSpout";
    String configFilename = "config.properties";

    Properties properties = new Properties();
    properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configFilename));
    builder.setSpout(spoutName,new TwitterSpout(
        properties.getProperty("consumerKey"),
        properties.getProperty("consumerSecret"),
        properties.getProperty("accessToken"),
        properties.getProperty("accessTokenSecret")
        ));
    builder.setBolt("printBolt", new PrintBolt()).shuffleGrouping(spoutName);

    Config conf = new Config();
    conf.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("twitterStorm", conf, builder.createTopology());

  }
}
