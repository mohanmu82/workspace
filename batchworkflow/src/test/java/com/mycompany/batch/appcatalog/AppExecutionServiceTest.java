package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The variables a use case's JSONata transform may read. {@code ${name}} is filled in from the same
 * merged map the URL and the body were built from; everything else about the expression — and every
 * {@code $} JSONata spends on itself — is left exactly as written.
 */
class AppExecutionServiceTest {

    private final AppExecutionService service = new AppExecutionService(null, new ObjectMapper(), null);

    @Test
    void bracedPlaceholder_isFilledInFromTheVariables() {
        String resolved = service.resolveExpression("data[id = \"${orderId}\"].lines", Map.of("orderId", "A-7"));
        assertThat(resolved).isEqualTo("data[id = \"A-7\"].lines");
    }

    @Test
    void severalPlaceholders_areAllFilledIn() {
        String resolved = service.resolveExpression("data[region = \"${region}\" and seq = ${seq}]",
                Map.of("region", "emea", "seq", 4));
        assertThat(resolved).isEqualTo("data[region = \"emea\" and seq = 4]");
    }

    @Test
    void valueGoesInAsItStands_soTheAuthorChoosesTheQuoting() {
        // Unquoted, a number stays a number and compares as one — which is the reason not to quote
        // on the caller's behalf.
        assertThat(service.resolveExpression("items[qty > ${min}]", Map.of("min", 3)))
                .isEqualTo("items[qty > 3]");
    }

    @Test
    void jsonatasOwnDollars_areLeftAlone() {
        String expression = "$sum(items.$map(lines, function($l) { $l.qty }))";
        assertThat(service.resolveExpression(expression, Map.of("items", "should not be used")))
                .isEqualTo(expression);
    }

    @Test
    void expressionWithoutPlaceholders_isUnchanged() {
        assertThat(service.resolveExpression("data.items.{\"id\": id}", Map.of()))
                .isEqualTo("data.items.{\"id\": id}");
    }

    @Test
    void unansweredPlaceholder_saysSoRatherThanReachingTheParser() {
        assertThatThrownBy(() -> service.resolveExpression("data[id = \"${orderId}\"]", Map.of("other", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("${orderId}")
                .hasMessageContaining("no value");
    }

    @Test
    void everyUnansweredPlaceholder_isNamedAtOnce() {
        assertThatThrownBy(() -> service.resolveExpression("data[a = ${one} and b = ${two}]", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("${one}")
                .hasMessageContaining("${two}");
    }

    @Test
    void nullAndEmptyExpressions_passStraightThrough() {
        assertThat(service.resolveExpression(null, Map.of())).isNull();
        assertThat(service.resolveExpression("", Map.of())).isEmpty();
    }
}
