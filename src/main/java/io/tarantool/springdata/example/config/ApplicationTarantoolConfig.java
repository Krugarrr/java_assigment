package io.tarantool.springdata.example.config;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.springdata.example.repository.ShipsRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.tarantool.config.AbstractTarantoolDataConfiguration;
import org.springframework.data.tarantool.core.convert.MappingTarantoolConverter;
import org.springframework.data.tarantool.core.convert.TarantoolCustomConversions;
import org.springframework.data.tarantool.core.convert.TarantoolMapTypeAliasAccessor;
import org.springframework.data.tarantool.core.mapping.TarantoolMappingContext;
import org.springframework.data.tarantool.repository.config.EnableTarantoolRepositories;

import javax.net.ssl.SSLException;
import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableTarantoolRepositories(basePackageClasses = {
    ShipsRepository.class,
})
class ApplicationTarantoolConfig extends AbstractTarantoolDataConfiguration {
    @Value("${tarantool.host}")
    protected String host;
    @Value("${tarantool.port}")
    protected int port;
    @Value("${tarantool.username}")
    protected String username;
    @Value("${tarantool.password}")
    protected String password;
    @Value("${client_certificate}")
    protected String clientCertificatePath;
    @Value("${client_certificate_key}")
    protected String clientCertificateKeyPath;
    @Value("${client_certificate_passphrase}")
    protected String clientCertificatePassphrase;
    @Value("${ca_certificate}")
    protected String caCertificatePath;

    @Override
    protected TarantoolServerAddress tarantoolServerAddress() {
        return new TarantoolServerAddress(host, port);
    }

    @Override
    public TarantoolCredentials tarantoolCredentials() {
        return new SimpleTarantoolCredentials(username, password);
    }

    public SslContext createSslContext() {
        try {
            return SslContextBuilder.forClient()
                .trustManager(new File(caCertificatePath))
                .keyManager(new File(clientCertificatePath), new File(clientCertificateKeyPath),
                    clientCertificatePassphrase)
                .build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void configureClientConfig(TarantoolClientConfig.Builder builder) {
        super.configureClientConfig(builder);
        builder.withSslContext(createSslContext());
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>
    tarantoolClient(
        TarantoolClientConfig tarantoolClientConfig,
        TarantoolClusterAddressProvider tarantoolClusterAddressProvider) {
        return TarantoolClientFactory.createClient()
            .withTarantoolClientConfig(tarantoolClientConfig)
            .withAddressProvider(tarantoolClusterAddressProvider)
            .withProxyMethodMapping()
            .build();
    }

    @Override
    public MappingTarantoolConverter mappingTarantoolConverter(TarantoolMappingContext tarantoolMappingContext,
        TarantoolMapTypeAliasAccessor typeAliasAccessor,
        TarantoolCustomConversions tarantoolCustomConversions) {
        return new MappingTarantoolConverter(tarantoolMappingContext, typeAliasAccessor, tarantoolCustomConversions);
    }

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
