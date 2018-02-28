package de.codecentric.kafka.demo.avro.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
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
import de.codecentric.kafka.demo.avro.producer.AvroProducer;
import de.codecentric.kafka.demo.avro.streams.KafkaStreamsConfiguration;
import de.codecentric.kafka.demo.embeddedkafka.ContainerTestUtils;
import de.codecentric.kafka.demo.embeddedkafka.EmbeddedSingleNodeKafkaCluster;
import de.codecentric.kafka.demo.embeddedkafka.IntegrationTestUtils;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

@RunWith(SpringRunner.class)
@SpringBootTest
public class StreamTests {
	
	@Autowired
	private AvroProducer producer;
	
	@Autowired
	private KafkaStreamsConfiguration kStreamConf;

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
		
		KStreamBuilder ksb = new KStreamBuilder();
		KStream<String, String> stream = kStreamConf.kStream(ksb);

		producer.send("lea", new User("lea", 28, "pink"), "topic1");
		producer.send("vader", new User("vader", 52, "black"), "topic1");
		producer.send("luke", new User("luke", 32, "blue"), "topic1");
		
		producer.send("luke", new User("luke", 32, "blue"), "topic2");
		producer.send("vader", new User("vader", 52, "black"), "topic2");
		producer.send("han", new User("han", 43, "grey"), "topic2");
		producer.send("boba", new User("boba", -1, "yellow"), "topic2");

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "streamtest");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    List<KeyValue<String, String>> actualWordCounts = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(props,
	        "topic3", 2);
	    assertThat(actualWordCounts).size().isEqualTo(2);
	}

}
