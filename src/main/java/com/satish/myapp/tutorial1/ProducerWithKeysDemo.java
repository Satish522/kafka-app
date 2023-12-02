package com.satish.myapp.tutorial1;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeysDemo {

	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
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

		//Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		
		for(int i=0;i < 10;i++) {
			
			String topic = "first_topic";
			String value = "Hello Bro, I am back"+i;
			String key = "id_"+i;
			
			
			//Create record to send to consumer
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);
			
			logger.info("Key: "+key);
			
			//Key: id_0  partition 1
			//Key: id_1  partition 0
			//Key: id_2  partition 2
			//Key: id_3  partition 0
			//Key: id_4  partition 2
			//Key: id_5  partition 2
			//Key: id_6  partition 0
			//Key: id_7  partition 2
			//Key: id_8  partition 1
			//Key: id_9  partition 2
			
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
			}).get();
			
			
		
		}
		
		producer.flush();
		
		producer.close();
		

	}

}
