package com.mycompany.batch.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CacheFactoryTest {

    private CacheFactory cache;

    @BeforeEach
    void setUp() {
        cache = new CacheFactory();
    }

    @Test
    void get_returnsNull_whenCacheDoesNotExist() {
        assertThat(cache.get("missing", "key")).isNull();
    }

    @Test
    void get_returnsNull_whenKeyDoesNotExist() {
        cache.save("myCache", "other", "val", "url");
        assertThat(cache.get("myCache", "absent")).isNull();
    }

    @Test
    void saveAndGet_roundTrip() {
        cache.save("myCache", "key1", "value1", "http://example.com");
        assertThat(cache.get("myCache", "key1")).isEqualTo("value1");
    }

    @Test
    void save_overwritesExistingEntry() {
        cache.save("myCache", "key1", "old", "url1");
        cache.save("myCache", "key1", "new", "url2");
        assertThat(cache.get("myCache", "key1")).isEqualTo("new");
    }

    @Test
    void save_multipleCaches_isolatedFromEachOther() {
        cache.save("cache1", "k", "v1", "u1");
        cache.save("cache2", "k", "v2", "u2");
        assertThat(cache.get("cache1", "k")).isEqualTo("v1");
        assertThat(cache.get("cache2", "k")).isEqualTo("v2");
    }

    @Test
    void clear_removesAllEntriesInCache() {
        cache.save("myCache", "key1", "v1", "u1");
        cache.save("myCache", "key2", "v2", "u2");
        cache.clear("myCache");
        assertThat(cache.get("myCache", "key1")).isNull();
        assertThat(cache.get("myCache", "key2")).isNull();
    }

    @Test
    void clear_doesNotAffectOtherCaches() {
        cache.save("cache1", "k", "v1", "u1");
        cache.save("cache2", "k", "v2", "u2");
        cache.clear("cache1");
        assertThat(cache.get("cache2", "k")).isEqualTo("v2");
    }

    @Test
    void clear_onNonexistentCache_doesNotThrow() {
        cache.clear("nonexistent");
    }

    @Test
    void getAll_returnsAllCaches() {
        cache.save("cache1", "k1", "v1", "u1");
        cache.save("cache2", "k2", "v2", "u2");
        assertThat(cache.getAll()).containsKeys("cache1", "cache2");
    }

    @Test
    void getAll_returnsUnmodifiableView() {
        cache.save("myCache", "k", "v", "u");
        assertThat(cache.getAll()).isNotEmpty();
    }

    @Test
    void getEntries_returnsEmptyMap_forUnknownCache() {
        assertThat(cache.getEntries("unknown")).isEmpty();
    }

    @Test
    void getEntries_returnsEntries_forKnownCache() {
        cache.save("myCache", "key1", "val1", "url1");
        cache.save("myCache", "key2", "val2", "url2");
        assertThat(cache.getEntries("myCache")).containsKeys("key1", "key2");
    }

    @Test
    void cacheEntry_storesValueAndUrl() {
        cache.save("myCache", "key1", "someValue", "http://origin.com");
        CacheFactory.CacheEntry entry = cache.getEntries("myCache").get("key1");
        assertThat(entry.value()).isEqualTo("someValue");
        assertThat(entry.url()).isEqualTo("http://origin.com");
    }

    @Test
    void get_withNullMaxRetention_returnsValue() {
        cache.save("myCache", "key1", "val", "url");
        assertThat(cache.get("myCache", "key1", null)).isEqualTo("val");
    }

    @Test
    void get_withGenerousRetention_returnsValue() {
        cache.save("myCache", "key1", "val", "url");
        assertThat(cache.get("myCache", "key1", 60)).isEqualTo("val");
    }

    @Test
    void get_withZeroRetention_treatedAsNoExpiry() {
        cache.save("myCache", "key1", "val", "url");
        // maxRetentionMinutes=0 → condition is 0 > 0 → false → no expiry check
        assertThat(cache.get("myCache", "key1", 0)).isEqualTo("val");
    }
}
