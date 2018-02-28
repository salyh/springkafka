package de.codecentric.kafka.demo.avro.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.junit4.SpringRunner;

import de.codecentric.kafka.demo.avro.User;
import de.codecentric.kafka.demo.avro.consumer.AvroConsumer;
import de.codecentric.kafka.demo.avro.producer.AvroProducer;
import de.codecentric.kafka.demo.embeddedkafka.ContainerTestUtils;
import de.codecentric.kafka.demo.embeddedkafka.EmbeddedSingleNodeKafkaCluster;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaAvroDemoTests {
	
	@Autowired
	private AvroProducer producer;

	@Autowired
	private AvroConsumer consumer;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	
	private static EmbeddedSingleNodeKafkaCluster kafka = new EmbeddedSingleNodeKafkaCluster();

	@BeforeClass
	public static void setUpClass() throws Exception {
		kafka.start();
		kafka.createTopic("avromessages", 1, 1);
		kafka.createTopic("topic1", 1, 1);
		kafka.createTopic("topic2", 1, 1);
		kafka.createTopic("topic3", 1, 1);
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		kafka.stop();
	}
	
	@Before
	public void setUp() throws Exception {		
		// wait until the partitions are assigned
		for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
				.getListenerContainers()) {
			
		     ContainerTestUtils.waitForAssignment(messageListenerContainer, 1);
		}
	}
	
	@Test
	public void testReceive() throws Exception {
		
		User user = new User("luke", 42, "blue");
		producer.send(user);

		consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
		assertThat(consumer.getLatch().getCount()).isEqualTo(0);
		
		Thread.sleep(10000);
	}

}
