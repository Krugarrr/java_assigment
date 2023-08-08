package io.tarantool.springdata.example.repository;

import java.nio.BufferUnderflowException;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import lombok.var;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang.NotImplementedException;

import static java.lang.Math.toIntExact;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.springdata.example.AbstractBaseIntegrationTest;
import io.tarantool.springdata.example.model.Ship;

@Testcontainers
@TestMethodOrder(MethodOrderer.Random.class)
public class ShipTest extends AbstractBaseIntegrationTest {
    //ARRANGE
    final static Logger log = LoggerFactory.getLogger(AbstractBaseIntegrationTest.class);

    @Container
    private static final TarantoolContainer tt = new TarantoolContainer().withLogConsumer(new Slf4jLogConsumer(log));

    private static Ship tuple;
    private static final String spaceName = "ships";

    private final ShipsRepository repository;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private final TarantoolClient client;

    @Autowired
    public ShipTest(ShipsRepository repository, TarantoolClient client) {
        this.repository = repository;
        this.client = client;
    }

    @BeforeAll
    static void beforeAll() {
        tuple = Ship.builder()
                .id(1)
                .name("Lesnoe")
                .crew(800)
                .gunsCount(90)
                .createdAt(Instant.EPOCH)
                .build();
    }

    @BeforeEach
    void beforeEach() {
        repository.deleteAll();
    }

    @AfterEach
    void afterEach() {
        repository.deleteAll();
    }

    @Test
    public void testEval() throws BufferUnderflowException, ExecutionException, InterruptedException {
        //ACT
        if (tuple.getCreatedAt() == Instant.EPOCH) {
            var epochDate = tuple.getCreatedAt().getEpochSecond();
            assertEquals(toIntExact(tuple.getCreatedAt().toEpochMilli()), client.eval("return ...", Collections.singletonList(epochDate)).get().get(0));

        }
        else {
            //ASSERT
            assertEquals(tuple.getCreatedAt(), client.eval("return ...", Collections.singletonList(tuple.getCreatedAt())).get().get(0));
        }
    }

    @Test
    public void testSaveAndFind() throws ExecutionException, InterruptedException {
        //ACT
        Ship expectedTuple = tuple;
        Ship actualTuple = repository.save(tuple);
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space = client.space(spaceName);

        TarantoolResult<TarantoolTuple> actualTuple2 = space.select(Conditions.equals("id", 1)).get();
        TarantoolTuple selectedTuple = actualTuple2.get(0);

        //ASSERT
        assertTrue(selectedTuple.getField(0).isPresent());
        assertEquals(expectedTuple, actualTuple);
        assertEquals(expectedTuple.getId(), selectedTuple.getInteger("id"));
    }

    @Test
    public void insertShip() throws ExecutionException, InterruptedException {
        //ARRANGE
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space = client.space(spaceName);
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(client.getConfig().getMessagePackMapper());
        TarantoolTuple shipsTuple = tupleFactory
                .create(
                        tuple.getId(),
                        tuple.getName(),
                        tuple.getCrew(),
                        tuple.getGunsCount(),
                        tuple.getCreatedAt(),
                        tuple.getBreadth());

        //ACT
        TarantoolResult<TarantoolTuple> insertionTuple = space.insert(shipsTuple).get();
        TarantoolTuple tarantoolTuple = insertionTuple.get(0);

        //ASSERT
        assertEquals(insertionTuple.size(), 1);
        assertEquals(6, tarantoolTuple.size());
    }
}
