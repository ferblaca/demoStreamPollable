package com.example.demoStreamPollable;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
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

/**
 * Functional consumers are instrumented by Spring Integration ({@code spring.integration.send} on the binding
 * channel), while consumers using {@link PollableMessageSource} produce no Spring Integration meters for their
 * binding, neither on success nor on failure.
 * Only the error bridge of the pollable binding ({@code <binder>.<binding>.errors.bridge}) is instrumented.
 * <p>
 * {@code pollableConsumerIsInstrumentedBySpringIntegration} fails with the released Spring Cloud Stream version.
 */
@Testcontainers
@SpringBootTest(classes = PollableSpringIntegrationMetricsTests.TestConfig.class, properties = {
		"spring.cloud.stream.defaultBinder=kafka",
		"spring.cloud.function.definition=functionalInput",
		"spring.cloud.stream.pollable-source=pollableInput",
		"spring.cloud.stream.bindings.functionalInput-in-0.destination=" + PollableSpringIntegrationMetricsTests.FUNCTIONAL_TOPIC,
		"spring.cloud.stream.bindings.functionalInput-in-0.group=functional-group",
		"spring.cloud.stream.bindings.pollableInput-in-0.destination=" + PollableSpringIntegrationMetricsTests.POLLABLE_TOPIC,
		"spring.cloud.stream.bindings.pollableInput-in-0.group=pollable-group",
		"spring.cloud.stream.bindings.pollableInput-in-0.consumer.max-attempts=1"})
class PollableSpringIntegrationMetricsTests {

	static final String FUNCTIONAL_TOPIC = "scs-functional-si-metrics";

	static final String POLLABLE_TOPIC = "scs-pollable-si-metrics";

	static final AtomicBoolean FUNCTIONAL_HANDLED = new AtomicBoolean();

	@Container
	static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

	@DynamicPropertySource
	static void kafkaProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
	}

	@BeforeAll
	static void createTopics() throws Exception {
		try (var admin = Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
			admin.createTopics(List.of(new NewTopic(FUNCTIONAL_TOPIC, 1, (short) 1),
					new NewTopic(POLLABLE_TOPIC, 1, (short) 1))).all().get();
		}
	}

	@jakarta.annotation.Resource
	@Qualifier("pollableInput-in-0")
	private PollableMessageSource pollableMessageSource;

	@jakarta.annotation.Resource
	private MeterRegistry meterRegistry;

	@Test
	void functionalConsumerIsInstrumentedBySpringIntegration() throws Exception {
		sendMessage(FUNCTIONAL_TOPIC);

		Awaitility.await().atMost(Duration.ofSeconds(15)).untilTrue(FUNCTIONAL_HANDLED);
		Awaitility.await().atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(this.meterRegistry.find("spring.integration.send")
						.tag("name", "functionalInput-in-0").tag("result", "success").timer())
						.isNotNull()
						.satisfies(timer -> assertThat(timer.count()).isEqualTo(1)));
	}

	@Test
	void pollableConsumerIsInstrumentedBySpringIntegration() throws Exception {
		sendMessage(POLLABLE_TOPIC);
		sendMessage(POLLABLE_TOPIC);

		// successful poll
		Awaitility.await().atMost(Duration.ofSeconds(15))
				.until(() -> this.pollableMessageSource.poll(message -> {
				}));

		// failed poll: the handler throws
		Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
			try {
				return this.pollableMessageSource.poll(message -> {
					throw new IllegalStateException("handler failure");
				});
			}
			catch (Exception ex) {
				return true;
			}
		});

		dumpIntegrationMeters();

		// The error bridge of the pollable binding is instrumented by Spring Integration...
		assertThat(this.meterRegistry.find("spring.integration.send").tag("type", "handler").timers())
				.as("error bridge handler timer for the pollable binding")
				.anyMatch(timer -> timer.getId().getTag("name").endsWith(".pollableInput-in-0.errors.bridge"));

		// ...but the pollable binding itself is not.
		// Expected: the pollable binding is instrumented like any other Spring Integration endpoint.
		// Actual (Spring Cloud Stream 5.0.x): no "spring.integration.*" meter is recorded for "pollableInput-in-0".
		assertThat(this.meterRegistry.find("spring.integration.receive").tag("name", "pollableInput-in-0").counter())
				.as("receive counter for the pollable binding")
				.isNotNull()
				.satisfies(counter -> assertThat(counter.count()).isEqualTo(2));
		assertThat(this.meterRegistry.find("spring.integration.send").tag("name", "pollableInput-in-0")
				.tag("result", "success").timer())
				.as("successful handling timer for the pollable binding")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).isEqualTo(1));
		assertThat(this.meterRegistry.find("spring.integration.send").tag("name", "pollableInput-in-0")
				.tag("result", "failure").timer())
				.as("failed handling timer for the pollable binding")
				.isNotNull()
				.satisfies(timer -> assertThat(timer.count()).isEqualTo(1));
	}

	private void dumpIntegrationMeters() {
		this.meterRegistry.getMeters().stream()
				.map(Meter::getId)
				.filter(id -> id.getName().startsWith("spring.integration."))
				.forEach(id -> System.out.println("METER " + id.getName() + " " + id.getTags()));
	}

	private static void sendMessage(final String topic) throws Exception {
		final Map<String, Object> producerConfig = new HashMap<>();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		try (var producer = new KafkaProducer<String, String>(producerConfig)) {
			producer.send(new ProducerRecord<>(topic, "payload")).get();
		}
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	static class TestConfig {

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		Consumer<String> functionalInput() {
			return payload -> FUNCTIONAL_HANDLED.set(true);
		}

	}

}
