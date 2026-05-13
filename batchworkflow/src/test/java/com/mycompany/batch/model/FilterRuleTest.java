package com.mycompany.batch.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FilterRuleTest {

    @Test
    void defaultOperation_isEq() {
        FilterRule rule = new FilterRule();
        assertThat(rule.getOperation()).isEqualTo("eq");
    }

    @Test
    void defaultColumnAndValue_areNull() {
        FilterRule rule = new FilterRule();
        assertThat(rule.getColumn()).isNull();
        assertThat(rule.getValue()).isNull();
    }

    @Test
    void setColumn_andGetColumn() {
        FilterRule rule = new FilterRule();
        rule.setColumn("status");
        assertThat(rule.getColumn()).isEqualTo("status");
    }

    @Test
    void setValue_andGetValue() {
        FilterRule rule = new FilterRule();
        rule.setValue("ACTIVE");
        assertThat(rule.getValue()).isEqualTo("ACTIVE");
    }

    @Test
    void setOperation_andGetOperation() {
        FilterRule rule = new FilterRule();
        rule.setOperation("like");
        assertThat(rule.getOperation()).isEqualTo("like");
    }

    @Test
    void allSetters_workTogether() {
        FilterRule rule = new FilterRule();
        rule.setColumn("region");
        rule.setValue("^US.*");
        rule.setOperation("like");
        assertThat(rule.getColumn()).isEqualTo("region");
        assertThat(rule.getValue()).isEqualTo("^US.*");
        assertThat(rule.getOperation()).isEqualTo("like");
    }
}
