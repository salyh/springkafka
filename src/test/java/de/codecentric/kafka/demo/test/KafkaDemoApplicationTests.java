package de.codecentric.kafka.demo.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.junit4.SpringRunner;

import de.codecentric.kafka.demo.avro.User;
import de.codecentric.kafka.demo.consumer.AvroConsumer;
import de.codecentric.kafka.demo.embeddedkafka.ContainerTestUtils;
import de.codecentric.kafka.demo.embeddedkafka.EmbeddedSingleNodeKafkaCluster;
import de.codecentric.kafka.demo.producer.AvroProducer;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaDemoApplicationTests {
	
	@Autowired
	private AvroProducer producer;

	@Autowired
	private AvroConsumer consumer;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	
	private EmbeddedSingleNodeKafkaCluster kafka = new EmbeddedSingleNodeKafkaCluster();

	@Before
	public void setUp() throws Exception {

		kafka.start();		
		
		//Thread.sleep(20000);
		
		// wait until the partitions are assigned
		for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
				.getListenerContainers()) {
			
		     ContainerTestUtils.waitForAssignment(messageListenerContainer, 1);
		}
	}
	
	@After
	public void tearDown() {
		System.out.println("SHUTDOWN");
		kafka.stop();
	}

	@Test
	public void testReceive() throws Exception {
		
		User user = new User("luke", 42, "blue");
		producer.send(user);

		consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
		assertThat(consumer.getLatch().getCount()).isEqualTo(0);
	}

}
