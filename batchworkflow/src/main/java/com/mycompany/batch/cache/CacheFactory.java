package com.mycompany.batch.cache;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple in-memory cache keyed by {@code cacheName → key → CacheEntry}.
 *
 * <p>Thread-safe: uses {@link ConcurrentHashMap} at both levels.
 * Entries live until the application restarts or a caller invokes {@link #clear(String)},
 * unless a {@code maxRetentionMinutes} TTL is provided at read time.
 */
@Component
public class CacheFactory {

    /** @param savedAtMs wall-clock time (ms) when the entry was written */
    public record CacheEntry(String value, String url, long savedAtMs) {
        /** Backward-compatible constructor for callers that don't supply a timestamp. */
        public CacheEntry(String value, String url) {
            this(value, url, System.currentTimeMillis());
        }
    }

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, CacheEntry>> store =
            new ConcurrentHashMap<>();

    /**
     * Returns the cached value for the given key, or {@code null} if absent.
     * No TTL check — entries are considered perpetually valid.
     */
    public String get(String cacheName, String key) {
        return get(cacheName, key, null);
    }

    /**
     * Returns the cached value for the given key, or {@code null} if absent or expired.
     *
     * @param maxRetentionMinutes maximum age in minutes; {@code null} means no expiry
     */
    public String get(String cacheName, String key, Integer maxRetentionMinutes) {
        ConcurrentHashMap<String, CacheEntry> cache = store.get(cacheName);
        if (cache == null) return null;
        CacheEntry entry = cache.get(key);
        if (entry == null) return null;
        if (maxRetentionMinutes != null && maxRetentionMinutes > 0) {
            long ageMs = System.currentTimeMillis() - entry.savedAtMs();
            if (ageMs > (long) maxRetentionMinutes * 60_000L) return null;
        }
        return entry.value();
    }

    /** Stores a response body under the given cache name and key, recording the current time. */
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
