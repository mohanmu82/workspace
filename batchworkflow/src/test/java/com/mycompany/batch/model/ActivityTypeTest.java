package com.mycompany.batch.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActivityTypeTest {

    @Test
    void from_null_returnsNull() {
        assertThat(ActivityType.from(null)).isNull();
    }

    @Test
    void from_blank_returnsNull() {
        assertThat(ActivityType.from("   ")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"HTTP", "http", "Http", "hTtP"})
    void from_http_caseInsensitive(String input) {
        assertThat(ActivityType.from(input)).isEqualTo(ActivityType.HTTP);
    }

    @ParameterizedTest
    @ValueSource(strings = {"DATAEXTRACTION", "dataextraction", "DataExtraction"})
    void from_dataExtraction_caseInsensitive(String input) {
        assertThat(ActivityType.from(input)).isEqualTo(ActivityType.DATAEXTRACTION);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HTTP", "DATAEXTRACTION", "DB", "SSH", "TRANSFORM", "VALIDATION", "DATAENRICHER"})
    void from_allValidValues_parsedSuccessfully(String value) {
        assertThat(ActivityType.from(value)).isNotNull();
    }

    @Test
    void from_unknownValue_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ActivityType.from("UNKNOWN"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UNKNOWN")
                .hasMessageContaining("Unknown activity type");
    }

    @Test
    void from_withLeadingTrailingSpaces_trimmedAndParsed() {
        assertThat(ActivityType.from("  DB  ")).isEqualTo(ActivityType.DB);
    }

    @Test
    void values_containsAllDocumentedTypes() {
        assertThat(ActivityType.values())
                .extracting(Enum::name)
                .contains("HTTP", "DATAEXTRACTION", "DB", "SSH", "TRANSFORM", "VALIDATION", "DATAENRICHER");
    }
}
