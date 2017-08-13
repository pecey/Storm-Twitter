package com.pecey.util;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout implements IRichSpout {
  private SpoutOutputCollector collector;
  private LinkedBlockingQueue<Status> queue;
  private TwitterStream twitterStream;

  private String consumerKey;
  private String consumerSecret;
  private String accessTokenSecret;
  private String accessToken;
  public TwitterSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.accessToken = accessToken;
    this.accessTokenSecret = accessTokenSecret;
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    collector = spoutOutputCollector;
    queue = new LinkedBlockingQueue<Status>(1000);

    StatusListener streamListener = new StatusListener() {
      @Override
      public void onStatus(Status status) {
        queue.offer(status);
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
      }

      @Override
      public void onTrackLimitationNotice(int i) {
      }

      @Override
      public void onScrubGeo(long l, long l1) {
      }

      @Override
      public void onStallWarning(StallWarning stallWarning) {
      }

      @Override
      public void onException(Exception e) {
      }
    };
    twitterStream = new TwitterStreamFactory(getConfig()).getInstance();
    twitterStream.addListener(streamListener);
    twitterStream.sample();
  }

  @Override
  public void close() {

  }

  @Override
  public void activate() {

  }

  @Override
  public void deactivate() {

  }

  @Override
  public void nextTuple() {
    Status ret = queue.poll();
    if (ret == null) {
      Utils.sleep(50);
    } else {
      collector.emit(new Values(ret));
    }
  }

  @Override
  public void ack(Object o) {

  }

  @Override
  public void fail(Object o) {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("tweet"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }


  private Configuration getConfig(){
    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(consumerSecret)
        .setOAuthAccessToken(accessToken)
        .setOAuthAccessTokenSecret(accessTokenSecret);

    return configurationBuilder.build();
  }



}
