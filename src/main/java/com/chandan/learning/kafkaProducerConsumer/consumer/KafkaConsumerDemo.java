package com.chandan.learning.kafkaProducerConsumer.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemo
{

	private static Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);

	public static void main(String[] args)
	{
		String bootStrapServers = "localhost:9092";

		String topicName = "first_topic";

		String consumerGroupId = "my-second-consumer-group";

		Properties producerProperties = new Properties();

		producerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);

		producerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		producerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		producerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

		producerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(producerProperties);)
		{
			consumer.subscribe(Arrays.asList(topicName));

			while (true)
			{
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

				consumerRecords.forEach(consumerRecord -> logger
						.info("key: " + consumerRecord.key() + "\nValue:  " + consumerRecord.value() + "\nPartition: "
								+ consumerRecord.partition() + "\nOffset: " + consumerRecord.offset()));
			}
		} finally
		{
			logger.info("Stopping Consumer..");
		}
	}
}
