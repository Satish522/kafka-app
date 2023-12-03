package com.satish.myapp.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) throws InterruptedException {
		 final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		 String groupId = "my_group";
		 
		 
		 Properties prop = new Properties();
		 prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		 prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		 prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		 prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		 
		 
		 KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		 
		 consumer.subscribe(Arrays.asList("first_topics"));
		
		 
		 while(true) {
			 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // 
			 for(ConsumerRecord<String, String> record : records ) {
				 //Thread.sleep(2000);
				 logger.info("Key: "+ record.key()+", Value: "+ record.value());
				 logger.info("Partition: "+ record.partition()+", Offset: "+ record.offset());
				 
			 }
			 
		
		}
		

	}

}
