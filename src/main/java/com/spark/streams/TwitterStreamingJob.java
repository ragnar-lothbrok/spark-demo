package com.spark.streams;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

//https://acadgild.com/blog/streaming-twitter-data-using-spark/
//https://dev.twitter.com/apps/12206303/oauth
public class TwitterStreamingJob {

	public static void main(String[] args) {

		String str[] = { "", "",
				"", "" };

		SparkConf sparkConf = new SparkConf().setMaster("local[3]").setAppName("Twitter Stream");

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(60000));

		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder().setOAuthConsumerKey(str[0]).setOAuthConsumerSecret(str[1])
				.setOAuthAccessToken(str[2]).setOAuthAccessTokenSecret(str[3]);

		OAuthAuthorization auth = new OAuthAuthorization(configurationBuilder.build());
		JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(streamingContext, auth);

		JavaPairDStream<String, Integer> pairStream = tweets.filter(new Function<Status, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Status v1) throws Exception {
				return v1.getLang().equals("en") && v1.getPlace() != null && v1.getPlace().getCountryCode().equalsIgnoreCase("IN");
			}
		}).flatMapToPair(new PairFlatMapFunction<Status, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(Status t) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
				HashtagEntity hashtagEntities[] = t.getHashtagEntities();
				for (HashtagEntity hashtagEntity : hashtagEntities) {
					list.add(new Tuple2<String, Integer>(hashtagEntity.getText(), 1));
				}
				return list;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		pairStream.print(10000);
		streamingContext.start();
		streamingContext.awaitTermination();

	}
}
