package com.chandan.learning.kafka.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer
{
	private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

	private String consumerKey = "";
	private String consumerSecret = "";
	private String token = "";
	private String secret = "";

	public static void main(String[] args)
	{
		System.out.println("Hello");
		new TwitterProducer().run();
	}

	public TwitterProducer()
	{
	}

	public void run()
	{
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(500);

		Client hosebirdClient = createTwitterClient(msgQueue);

		hosebirdClient.connect();

		KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

		while (!hosebirdClient.isDone())
		{
			String tweet = null;
			try
			{
				tweet = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
				hosebirdClient.stop();
			}
			if (tweet != null)
			{
				logger.info(tweet);
				kafkaProducer.send(new ProducerRecord<>("twitter-tweets", null, tweet), new Callback()
				{
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception)
					{
						if(exception != null)
						{
							logger.error("Something went wrong .." + exception.getMessage());
						}
					}
				});
			}
		}

	}

	private Client createTwitterClient(BlockingQueue<String> msgQueue)
	{

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		List<String> terms = Lists.newArrayList("bitcoins");

		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01").hosts(hosebirdHosts)
				.authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}

	private KafkaProducer<String, String> createKafkaProducer()
	{
		String bootStrapServers = "localhost:9092"; 

		Properties producerProperties = new Properties();

		//minimal producer prop
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//properties for idempotent producer
		producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		
		//properties for high throughput
		producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));//32KB max batch size
		producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

		

		return new KafkaProducer<>(producerProperties);

	}

}
