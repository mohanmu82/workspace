package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a page transform's JSONata comes from — the page itself, or the shared library by name. A
 * page is refused rather than saved half-resolving: a step naming an expression nobody has is a step
 * that silently does nothing, and a chain with a step missing from the middle produces a grid of the
 * wrong thing rather than an error anyone would see.
 */
class AppPageSharedTransformTest {

    private static final Predicate<String> LIBRARY_HAS = List.of("orderRows", "flatten")::contains;

    private static AppPageTransform jsonata(String name, String expression, String ref) {
        AppPageTransform transform = new AppPageTransform();
        transform.setName(name);
        transform.setType(AppPageTransform.JSONATA);
        transform.setExpression(expression);
        transform.setJsonataRef(ref);
        return transform;
    }

    @Test
    void aStepWithItsOwnExpression_isFine_asItAlwaysWas() {
        assertThatCode(() -> AppCatalogService.validateTransformExpression(
                jsonata("flattenRows", "data.items", null), LIBRARY_HAS))
                .doesNotThrowAnyException();
    }

    @Test
    void aStepNamingSomethingTheLibraryHolds_needsNoExpressionOfItsOwn() {
        assertThatCode(() -> AppCatalogService.validateTransformExpression(
                jsonata("flattenRows", null, "orderRows"), LIBRARY_HAS))
                .doesNotThrowAnyException();
    }

    @Test
    void aStepNamingSomethingTheLibraryHasNot_isRefusedWithTheNameInTheMessage() {
        assertThatThrownBy(() -> AppCatalogService.validateTransformExpression(
                jsonata("flattenRows", null, "goneAway"), LIBRARY_HAS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flattenRows")
                .hasMessageContaining("goneAway");
    }

    @Test
    void aStepWithNeither_isStillRefusedForHavingNoExpression() {
        assertThatThrownBy(() -> AppCatalogService.validateTransformExpression(
                jsonata("flattenRows", "   ", null), LIBRARY_HAS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no JSONata expression");
    }

    @Test
    void aBlankRefIsNoRef_soTheStepIsHeldToItsOwnExpression() {
        // The setter is what makes this true, and it matters: a browser that posts "" for a picker
        // nobody answered must not produce a step that claims to reference something.
        assertThatThrownBy(() -> AppCatalogService.validateTransformExpression(
                jsonata("flattenRows", null, "  "), LIBRARY_HAS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no JSONata expression");
    }

    @Test
    void anXmlStepIsDescribedByItsType_soNeitherIsAskedOfIt() {
        AppPageTransform xml = new AppPageTransform();
        xml.setName("parse");
        xml.setType(AppPageTransform.XML2JSON);

        assertThatCode(() -> AppCatalogService.validateTransformExpression(xml, LIBRARY_HAS))
                .doesNotThrowAnyException();
    }

    @Test
    void anXmlStepIsNeverALibraryReference_evenIfOneWasLeftOnIt() {
        AppPageTransform xml = new AppPageTransform();
        xml.setName("parse");
        xml.setType(AppPageTransform.XML2JSON);
        xml.setJsonataRef("orderRows");

        assertThatCode(() -> AppCatalogService.validateTransformExpression(xml, name -> false))
                .doesNotThrowAnyException();
    }
}
