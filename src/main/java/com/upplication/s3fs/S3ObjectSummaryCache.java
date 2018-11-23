package com.upplication.s3fs;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public enum S3ObjectSummaryCache {

    INSTANCE;

    private static final ConcurrentHashMap<String, S3ObjectSummary> cache = new ConcurrentHashMap<>();


    public S3ObjectSummary get(String s3Path) {
        return cache.get(s3Path);
    }

    public S3ObjectSummary put(String s3Path, S3ObjectSummary objectSummary) {
        return cache.put(s3Path, objectSummary);
    }

    public S3ObjectSummary remove(String s3Path) {
        return cache.remove(s3Path);
    }

    public Optional<S3ObjectSummary> getOrPutIfDirectory(String key, Supplier<S3ObjectSummary> supplier) {
        if (cache.containsKey(key)) {
            return Optional.of(cache.get(key));
        }

        return cache.keySet()
                .stream()
                .filter(k -> k.startsWith(key))
                .findFirst()
                .map(s -> cache.put(key, cache.get(s)));
    }
}
