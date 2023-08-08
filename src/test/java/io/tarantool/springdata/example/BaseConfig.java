package io.tarantool.springdata.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.tarantool.config.AbstractTarantoolDataConfiguration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;

/**
 * @author Artyom Dubinin
 */
public class BaseConfig extends AbstractTarantoolDataConfiguration {

	private static final Logger log = LoggerFactory.getLogger(AbstractTarantoolDataConfiguration.class);

	@Value("${tarantool.host}")
	protected String host;
	@Value("${tarantool.port}")
	protected int port;
	@Value("${tarantool.username}")
	protected String username;
	@Value("${tarantool.password}")
	protected String password;

	@Override
	public TarantoolCredentials tarantoolCredentials() {
		return new SimpleTarantoolCredentials(username, password);
	}

	@Override
	protected TarantoolServerAddress tarantoolServerAddress() {
		return new TarantoolServerAddress(host, port);
	}
}
