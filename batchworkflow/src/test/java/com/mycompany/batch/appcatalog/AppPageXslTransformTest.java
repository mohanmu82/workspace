package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AppPageXslTransformTest {

    private static final String XSL = """
            <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
              <xsl:output method="text"/>
              <xsl:template match="/">
                <xsl:text>id,name&#10;</xsl:text>
                <xsl:for-each select="//order">
                  <xsl:value-of select="@id"/>,"<xsl:value-of select="name"/>"<xsl:text>&#10;</xsl:text>
                </xsl:for-each>
              </xsl:template>
            </xsl:stylesheet>""";

    @Test
    void anXslStepTurnsXmlIntoGridRows() throws Exception {
        String xml = "<orders><order id=\"1\"><name>Smith, J</name></order><order id=\"2\"><name>Lee</name></order></orders>";

        assertThat(AppPageOutputReader.xslToCsvRows(xml, XSL, "toRows")).containsExactly(
                Map.of("id", "1", "name", "Smith, J"),
                Map.of("id", "2", "name", "Lee"));
    }

    @Test
    void csvHonoursQuotesAndSkipsBlankLines() {
        List<Map<String, String>> rows = AppPageOutputReader.csvRows("a,b\r\n\"x \"\"q\"\"\",\"1\n2\"\n\n3\n");

        assertThat(rows).containsExactly(Map.of("a", "x \"q\"", "b", "1\n2"), Map.of("a", "3", "b", ""));
    }

    @Test
    void anXslStepNeedsAStylesheet_andIsNeverALibraryReference() {
        AppPageTransform xsl = new AppPageTransform();
        xsl.setName("toRows");
        xsl.setType("xsl2csv");
        xsl.setJsonataRef("orderRows");

        assertThat(xsl.isXsl2Csv()).isTrue();
        assertThat(xsl.isLibraryRef()).isFalse();
        assertThatThrownBy(() -> AppCatalogService.validateTransformExpression(xsl, name -> true))
                .hasMessageContaining("no XSL stylesheet");
    }
}
