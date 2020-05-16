package com.chandan.learning.kafkaProducerConsumer.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerSeekAndAssignDemo
{
	private static Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);

	public static void main(String[] args)
	{

		String bootStrapServers = "localhost:9092";

		String topicName = "first_topic";

		Properties producerProperties = new Properties();

		producerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);

		producerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		producerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		TopicPartition topicPartition = new TopicPartition(topicName, 0);

		int messagesToRead = 5;

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(producerProperties);)
		{
			consumer.assign(Arrays.asList(topicPartition));
			
			consumer.seek(topicPartition, 5L);

			while (messagesToRead > 0)
			{
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				

				for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
				{
					logger.info("key: " + consumerRecord.key() + "\nValue:  " + consumerRecord.value() + "\nPartition: "
							+ consumerRecord.partition() + "\nOffset: " + consumerRecord.offset());
					
					messagesToRead--;
					
					if(messagesToRead <=0)
					{
						break;
					}
				}
			}
		} finally
		{
			logger.info("Stopping Consumer..");
		}

	}

}
