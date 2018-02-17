package de.codecentric.kafka.demo.embeddedkafka;

public class Main {
	
	public static void main(String[] args) {
		EmbeddedSingleNodeKafkaCluster kafka = new EmbeddedSingleNodeKafkaCluster();
		try {
			kafka.start();
			System.out.println("Started");
			System.out.println("Bootstrap servers: "+kafka.bootstrapServers());
			System.out.println("Zookeeper servers: "+kafka.zookeeperConnect());
			System.out.println("Schema registry server: "+kafka.schemaRegistryUrl());
			kafka.createTopic("avromessages", 1, 1);
		} catch (Exception e) {
			e.printStackTrace();
			kafka.stop();
			System.exit(-1);
		}
	}
	
}
