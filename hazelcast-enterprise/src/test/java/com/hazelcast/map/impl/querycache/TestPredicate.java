package com.hazelcast.map.impl.querycache;

import com.hazelcast.query.Predicate;

import java.util.Map;

public class TestPredicate implements Predicate {

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return false;
    }
}