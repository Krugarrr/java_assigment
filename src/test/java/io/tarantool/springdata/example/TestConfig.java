package io.tarantool.springdata.example;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.tarantool.repository.config.EnableTarantoolRepositories;

import io.tarantool.springdata.example.repository.ShipsRepository;

/**
 * @author Artyom Dubinin
 */
@Configuration
@EnableTarantoolRepositories(basePackageClasses = {ShipsRepository.class})
@EnableAutoConfiguration
public class TestConfig extends BaseConfig {
	@WritingConverter
	public enum InstantToInstantConverter implements Converter<Instant, Instant> {
		INSTANCE;

		@Override
		public Instant convert(Instant source) {
			return source;
		}
	}

	@Override
	protected List<?> customConverters() {
		List<Converter<?, ?>> customConverters = new ArrayList<>();
		// by default cartridge-springdata convert all types from Jsr310 to primitives
		customConverters.add(InstantToInstantConverter.INSTANCE);
		return customConverters;
	}
}
