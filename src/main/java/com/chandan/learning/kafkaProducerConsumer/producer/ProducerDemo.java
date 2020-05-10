package com.chandan.learning.kafkaProducerConsumer.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo
{
	public static void main(String[] args)
	{
		String bootStrapServers = "localhost:9092";
		
		Properties producerProperties = new Properties();
		
		
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties); 
		
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Hello from Java Producer");
		kafkaProducer.send(producerRecord);
		
		kafkaProducer.close();
		
		
	}
}
