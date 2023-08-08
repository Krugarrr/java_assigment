package io.tarantool.springdata.example;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;

@SpringBootTest(classes = TestConfig.class)
public class AbstractBaseIntegrationTest {

	final static Logger log = LoggerFactory.getLogger(AbstractBaseIntegrationTest.class);

	@Container
	private static final TarantoolContainer tarantoolContainer =
		new TarantoolContainer().withLogConsumer(new Slf4jLogConsumer(log));

	@BeforeAll
	static void startContainer() {
		if (!tarantoolContainer.isRunning()) {
			tarantoolContainer.start();
		}
	}

	@DynamicPropertySource
	static void tarantoolProperties(DynamicPropertyRegistry registry) {
		registry.add("tarantool.host", tarantoolContainer::getHost);
		registry.add("tarantool.port", tarantoolContainer::getPort);
		registry.add("tarantool.username", tarantoolContainer::getUsername);
		registry.add("tarantool.password", tarantoolContainer::getPassword);
	}
}
