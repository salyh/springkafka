package de.codecentric.kafka.demo.avro.consumer;

import java.util.concurrent.CountDownLatch;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

public class AvroConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroConsumer.class);

	private CountDownLatch latch = new CountDownLatch(1);

	public CountDownLatch getLatch() {
		return latch;
	}


	//always read from start for kakfa.topic.avro and partition 0
	//@KafkaListener(topicPartitions = @TopicPartition(topic = "${kakfa.topic.avro:avromessages}"
	//		, partitionOffsets=@PartitionOffset(partition = "0", initialOffset = "0")))
	@KafkaListener(topics = "${kakfa.topic.avro:avromessages}")
	public void receive(ConsumerRecord<String, GenericRecord> record, Acknowledgment ack) {
		LOGGER.info("received record='{}'", record);
		latch.countDown();
		ack.acknowledge();
	}

}
