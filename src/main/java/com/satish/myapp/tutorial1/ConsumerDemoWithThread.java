package com.satish.myapp.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static void main(String[] args) throws InterruptedException {
		 
		 
		 new ConsumerDemoWithThread().run();	 
	
	}
	
	private ConsumerDemoWithThread() {
		
	}
	
	public void run() {
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
		 
		 String bootstrapServer = "localhost:9092";
		 
		 String groupId = "my_thread_group";
		 String topic = "first_topics";
		 
		 CountDownLatch latch = new CountDownLatch(1);
		 logger.info("Runnable with consumer thread");
		 Runnable task= new ConsumerThread(latch, groupId, topic, bootstrapServer);
		 
		 Thread t = new Thread(task);
		 t.start();
		 
		 Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			 logger.info("Catch the shutdown hook");
			 ((ConsumerThread)task).shutdown();
			 try {
					latch.await();
				} catch (InterruptedException e) {
					logger.error("Application closing and interupted");
				}
		 }));
		 
		 try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application closing and interupted");
		}
		 
	}
	
	public class ConsumerThread implements Runnable{
		
		private CountDownLatch latch;
		
		KafkaConsumer<String, String> consumer;
		
		Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getClass());
		
		public ConsumerThread(CountDownLatch latch, String groupId, String topic, String bootstrapServer) {
			this.latch = latch;
			 Properties prop = new Properties();
			 prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			 prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			 prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			 prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			 prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			 //prop.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)
			 
			 consumer = new KafkaConsumer<String, String>(prop);
			 consumer.subscribe(Arrays.asList(topic));
			
		}

		public void run() {
			try {
				while(true) {
					 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // 
					 for(ConsumerRecord<String, String> record : records ) {
						 //Thread.sleep(2000);
						 logger.info("Key: "+ record.key()+", Value: "+ record.value());
						 logger.info("Partition: "+ record.partition()+", Offset: "+ record.offset());
						 
					 }
				
				}
			}catch (WakeupException e) {
				logger.info("Recieved shhutdown signal");
			} finally {
				consumer.close();
				latch.countDown();
			}
			
		}
		
		public void shutdown() {
			consumer.wakeup();
		}
	}

}
