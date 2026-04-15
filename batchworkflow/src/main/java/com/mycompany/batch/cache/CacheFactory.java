package com.mycompany.batch.cache;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple in-memory cache keyed by {@code cacheName → key → CacheEntry}.
 *
 * <p>Thread-safe: uses {@link ConcurrentHashMap} at both levels.
 * The cache is intentionally never expired automatically — entries live until
 * the application restarts or a caller invokes {@link #clear(String)}.
 */
@Component
public class CacheFactory {

    public record CacheEntry(String value, String url) {}

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, CacheEntry>> store =
            new ConcurrentHashMap<>();

    /**
     * Returns the cached response body for the given cache name and key,
     * or {@code null} if not present.
     */
    public String get(String cacheName, String key) {
        ConcurrentHashMap<String, CacheEntry> cache = store.get(cacheName);
        if (cache == null) return null;
        CacheEntry entry = cache.get(key);
        return entry != null ? entry.value() : null;
    }

    /** Stores a response body under the given cache name and key. */
    public void save(String cacheName, String key, String value, String url) {
        store.computeIfAbsent(cacheName, k -> new ConcurrentHashMap<>())
             .put(key, new CacheEntry(value, url));
    }

    /**
     * Removes all entries for the given cache name.
     * Does nothing if the cache does not exist.
     */
    public void clear(String cacheName) {
        store.remove(cacheName);
    }

    /** Returns an unmodifiable view of all caches and their entries. */
    public Map<String, ConcurrentHashMap<String, CacheEntry>> getAll() {
        return Collections.unmodifiableMap(store);
    }

    /**
     * Returns an unmodifiable view of all entries inside one cache.
     * Returns an empty map if the cache does not exist.
     */
    public Map<String, CacheEntry> getEntries(String cacheName) {
        ConcurrentHashMap<String, CacheEntry> cache = store.get(cacheName);
        return cache != null ? Collections.unmodifiableMap(cache) : Map.of();
    }
}
