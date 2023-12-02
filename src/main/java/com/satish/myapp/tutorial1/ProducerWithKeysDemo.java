package com.satish.myapp.tutorial1;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeysDemo {

	
	public static void main(String[] args) {
		System.out.println("Hello World , Welcome to Kafka app");
		final Logger logger = LoggerFactory.getLogger(ProducerWithKeysDemo.class); 
		
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
		int i=0;
		Random random = new Random();
		while(i < 5) {
			
			String topic = "first_topic";
			String value = "Hello Bro, I am back"+(++i);
			String key = "id_"+i;
			//Create Producer
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
			
			//Create record to send to consumer
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);
			//Send data
		
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						logger.info("Recieved new info. \n" +
								"Topic: "+ metadata.topic() + "\n" +
								"Partition: "+ metadata.partition()+  "\n" +
								"Offset: "+ metadata.offset()+  "\n" +
								"Timestamp: "+ metadata.timestamp()  
								);
					} else {
						logger.error("Record could not send", exception);
					}
					
				}
			});
			
			producer.flush();
			
			producer.close();
		
		}
		

	}

}
