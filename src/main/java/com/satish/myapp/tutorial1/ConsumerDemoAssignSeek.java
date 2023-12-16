package com.satish.myapp.tutorial1;

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

public class ConsumerDemoAssignSeek {

	public static void main(String[] args) throws InterruptedException {
		 final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		 String groupId = "my_first_group";
		 String topic = "assign_seek";
		 
		 Properties prop = new Properties();
		 prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		 prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		 prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		 //prop.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)
		 
		 KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		 
		 //consumer.subscribe(Arrays.asList("first_topics"));
		
		 
		 //Assign and Seek are mostly used to replay data specific range
		 TopicPartition partitionReadFrom = new TopicPartition(topic, 0);
		 
		 long offsetToReadFrom = 1L;
		 
		 consumer.assign(Arrays.asList(partitionReadFrom));
		 
		 consumer.seek(partitionReadFrom, offsetToReadFrom);
		 
		 int numberMessageToRead = 15;
		 
		 boolean keepOnReading = true;
		 int numberMessageReadSoFar =0;
	 
		 
		 while(keepOnReading) {
			 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // 
			 for(ConsumerRecord<String, String> record : records ) {
				 numberMessageReadSoFar+=1;
				 //Thread.sleep(2000);
				 logger.info("Key: "+ record.key()+", Value: "+ record.value());
				 logger.info("Partition: "+ record.partition()+", Offset: "+ record.offset());
				 
				 if(numberMessageReadSoFar >= numberMessageToRead) {
					 keepOnReading=false;
					 break;
				 }
				 
			 }
			 
		
		}
		logger.info("Exiting application");
		

	}

}

