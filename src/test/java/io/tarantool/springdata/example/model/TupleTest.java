package io.tarantool.springdata.example.model;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperationBitwiseXor;
import io.tarantool.driver.api.tuple.operations.TupleOperationDelete;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.springdata.example.AbstractBaseIntegrationTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableBigIntegerValueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigInteger;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@TestMethodOrder(MethodOrderer.Random.class)
public class TupleTest extends AbstractBaseIntegrationTest {
    //ARRANGE
    final static Logger log = LoggerFactory.getLogger(AbstractBaseIntegrationTest.class);
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final MessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();

    private final BigInteger integer = BigInteger.TEN;
    private final ImmutableArrayValue values = ValueFactory.newArray(new ImmutableBigIntegerValueImpl(integer));
    private TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapper);

    @Test
    void checkTarantoolTuple() {
        //ASSERT
        assertTrue(tarantoolTuple.getField(0).isPresent());
        assertEquals(String.valueOf(integer), String.valueOf(tarantoolTuple.getObject(0).get()));
    }

    @Test
    void putObjectInTarantoolTuple() {
        //ACT
        tarantoolTuple.putObject(2, 228322);

        //ASSERT
        assertTrue(tarantoolTuple.getField(2).isPresent());
        assertEquals(228322, tarantoolTuple.getInteger(2));
    }

    @Test
    public void checkIllegalTupleOperations() {
        assertThrows(IllegalArgumentException.class, () -> new TupleOperationDelete(0, -1));
        assertThrows(IllegalArgumentException.class, () -> new TupleOperationBitwiseXor(0, -5));
    }
}
