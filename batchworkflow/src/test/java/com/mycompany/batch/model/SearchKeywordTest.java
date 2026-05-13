package com.mycompany.batch.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SearchKeywordTest {

    @Test
    void defaultType_isContains() {
        SearchKeyword kw = new SearchKeyword();
        assertThat(kw.getType()).isEqualTo("contains");
    }

    @Test
    void defaultValue_isNull() {
        SearchKeyword kw = new SearchKeyword();
        assertThat(kw.getValue()).isNull();
    }

    @Test
    void setValue_andGetValue() {
        SearchKeyword kw = new SearchKeyword();
        kw.setValue("alice");
        assertThat(kw.getValue()).isEqualTo("alice");
    }

    @Test
    void setType_andGetType() {
        SearchKeyword kw = new SearchKeyword();
        kw.setType("startsWith");
        assertThat(kw.getType()).isEqualTo("startsWith");
    }

    @Test
    void allSetters_workTogether() {
        SearchKeyword kw = new SearchKeyword();
        kw.setValue("test");
        kw.setType("regex");
        assertThat(kw.getValue()).isEqualTo("test");
        assertThat(kw.getType()).isEqualTo("regex");
    }
}
