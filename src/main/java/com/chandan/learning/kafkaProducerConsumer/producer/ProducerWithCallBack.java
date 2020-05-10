package com.chandan.learning.kafkaProducerConsumer.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallBack
{

	private static Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);

	public static void main(String[] args)
	{
		String bootStrapServers = "localhost:9092";

		Properties producerProperties = new Properties();

		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

		for (int i = 0; i < 12; i++)
		{
			sendMessage(kafkaProducer, i);
		}

		kafkaProducer.close();

	}

	private static void sendMessage(KafkaProducer<String, String> kafkaProducer, int i)
	{
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "" + i,
				"Hello from Java Producer " + i);
		kafkaProducer.send(producerRecord, new Callback()
		{
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception)
			{
				if (exception == null)
				{
					logger.info("Published Message: \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
							+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "TimeStamp: "
							+ metadata.timestamp() + "\n");
				}
			}
		});
	}

}
