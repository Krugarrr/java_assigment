package io.tarantool.springdata.example.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.tarantool.core.mapping.Field;
import org.springframework.data.tarantool.core.mapping.Tuple;

import java.time.Instant;

@Data
@Builder(toBuilder = true)
@Tuple("ships")
public class Ship {
    @Id
    private Integer id;
    private String name;
    @Field("guns_count")
    private Integer gunsCount;
    private Integer crew;
    @Field("created_at")
    private Instant createdAt;
    private Double breadth;
}
