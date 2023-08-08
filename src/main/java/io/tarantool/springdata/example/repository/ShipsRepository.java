package io.tarantool.springdata.example.repository;

import io.tarantool.springdata.example.model.Ship;
import org.springframework.data.tarantool.repository.TarantoolRepository;

public interface ShipsRepository extends TarantoolRepository<Ship, Integer> {
}
