package com.example.demoStreamPollable;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(classes = SpringCloudStreamPollableKafkaMetricsTests.TestConfig.class, properties = {
		"spring.cloud.stream.defaultBinder=kafka", "spring.cloud.stream.pollable-source=pollableInput;pollableInputMaxOne",
		"spring.cloud.stream.bindings.pollableInput-in-0.destination=scs-pollable-metrics",
		"spring.cloud.stream.bindings.pollableInput-in-0.group=scs-pollable-metrics-group",
		"spring.cloud.stream.kafka.bindings.pollableInput-in-0.consumer.configuration.max.poll.records=10",
		"spring.cloud.stream.bindings.pollableInputMaxOne-in-0.destination=scs-pollable-metrics-max-one",
		"spring.cloud.stream.bindings.pollableInputMaxOne-in-0.group=scs-pollable-metrics-max-one-group",
		"spring.cloud.stream.kafka.bindings.pollableInputMaxOne-in-0.consumer.configuration.max.poll.records=1"})
class SpringCloudStreamPollableKafkaMetricsTests {

	private static final String TOPIC = "scs-pollable-metrics";

	private static final String TOPIC_MAX_ONE = "scs-pollable-metrics-max-one";

	@Container
	static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

	@DynamicPropertySource
	static void kafkaProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
	}

	@BeforeAll
	static void createTopic() throws Exception {
		try (var admin = org.apache.kafka.clients.admin.Admin
				.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
			admin.createTopics(java.util.List.of(new org.apache.kafka.clients.admin.NewTopic(TOPIC, 1, (short) 1),
					new org.apache.kafka.clients.admin.NewTopic(TOPIC_MAX_ONE, 1, (short) 1))).all().get();
		}
	}

	@jakarta.annotation.Resource
	@Qualifier("pollableInput-in-0")
	private PollableMessageSource pollableMessageSource;

	@jakarta.annotation.Resource
	@Qualifier("pollableInputMaxOne-in-0")
	private PollableMessageSource pollableMessageSourceMaxOne;

	@jakarta.annotation.Resource
	private MeterRegistry meterRegistry;

	@Test
	void springCloudStreamPollableConsumersRegisterKafkaMetricsOnlyWhenMaxPollRecordsIsOne() throws Exception {
		this.sendMessage(TOPIC, "group-" + UUID.randomUUID());

		final AtomicReference<Object> receivedMessage = new AtomicReference<>();
		org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15))
				.until(() -> this.pollableMessageSource.poll(receivedMessage::set));

		assertThat(receivedMessage).isNotNull();
		assertThat(this.meterRegistry.getMeters())
				.noneMatch(meter -> meter.getId().getName().startsWith("kafka.consumer"));

		this.sendMessage(TOPIC_MAX_ONE, "group-max-one-" + UUID.randomUUID());
		receivedMessage.set(null);
		org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15))
				.until(() -> this.pollableMessageSourceMaxOne.poll(receivedMessage::set));

		assertThat(receivedMessage).isNotNull();
		assertThat(this.meterRegistry.getMeters())
				.anyMatch(meter -> meter.getId().getName().startsWith("kafka.consumer"));
	}

	private void sendMessage(final String topic, final String group) throws Exception {
		final Map<String, Object> producerConfig = new HashMap<>();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		try (var producer = new KafkaProducer<String, String>(producerConfig)) {
			producer.send(new ProducerRecord<>(topic, group, "payload")).get();
		}
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	static class TestConfig {

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

	}

}
