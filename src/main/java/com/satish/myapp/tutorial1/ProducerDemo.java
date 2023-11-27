package com.satish.myapp.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		System.out.println("Hello World , Welcome to Kafka app");
		//Create producer properties
		Properties properties =  new Properties();
		/* Hard coded properties 
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer",StringSerializer.class.getName());
		properties.setProperty("value.serializer",StringSerializer.class.getName());
		*/
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		while(true) {
			//Create Producer
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
			
			//Create record to send to consumer
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Bro, I am back");
			//Send data
		
			producer.send(record);
			
			producer.flush();
			
			producer.close();
		
		}
		

	}

}
