package com.upplication.s3fs;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public enum S3ObjectSummaryCache {

    INSTANCE;

    private static final ConcurrentHashMap<String, S3ObjectSummary> cache = new ConcurrentHashMap<>();

    public S3ObjectSummary get(String s3Path) {
        return cache.get(s3Path);
    }

    public S3ObjectSummary put(String s3Path, S3ObjectSummary objectSummary) {
        cache.put(s3Path, objectSummary);
        return objectSummary;
    }

    public S3ObjectSummary remove(String key) {
        return cache.remove(key);
    }

    public Optional<S3ObjectSummary> getOrCacheDirectory(String key) {

        return Optional.ofNullable(get(key))
                .map(Optional::of)
                .orElseGet(() -> cache.keySet()
                        .stream()
                        .filter(k -> k.startsWith(key))
                        .findFirst()
                        .map(s -> put(key, get(s))));
    }

}
