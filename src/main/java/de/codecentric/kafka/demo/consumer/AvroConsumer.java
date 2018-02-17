package de.codecentric.kafka.demo.consumer;

import java.util.concurrent.CountDownLatch;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

public class AvroConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroConsumer.class);

	private CountDownLatch latch = new CountDownLatch(1);

	public CountDownLatch getLatch() {
		return latch;
	}

	@KafkaListener(topics = "${kakfa.topic.avro:avromessages}")
	public void receive(ConsumerRecord<String, GenericRecord> record) {
		LOGGER.info("received record='{}'", record);
		latch.countDown();
	}

}
