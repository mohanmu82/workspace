package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A page control runs another page inside its parent: it has to name a page in the catalog, may not
 * name the page it sits on, and may not lead back to it through the child's own page controls.
 */
class AppPageChildPageTest {

    private final Map<String, AppPage> catalog = new HashMap<>();

    private static AppPageControl pageControl(String childPageName) {
        AppPageControl control = new AppPageControl();
        control.setControlId("p");
        control.setType("page");
        control.setChildPageName(childPageName);
        return control;
    }

    private AppPage page(String name, AppPageControl... controls) {
        AppPage page = new AppPage();
        page.setPageName(name);
        page.setControls(List.of(controls));
        return page;
    }

    private AppPage stored(String name, AppPageControl... controls) {
        AppPage page = page(name, controls);
        catalog.put(name, page);
        return page;
    }

    private void validate(AppPage page) {
        for (AppPageControl control : page.getControls()) {
            AppCatalogService.validateChildPage(page, control, "Control 'p'", catalog::get);
        }
    }

    @Test
    void acceptsAChildPageInTheCatalog() {
        stored("child");
        assertThatCode(() -> validate(page("parent", pageControl("child")))).doesNotThrowAnyException();
    }

    @Test
    void acceptsTheSameChildReachedTwiceWithoutALoop() {
        stored("leaf");
        stored("left", pageControl("leaf"));
        stored("right", pageControl("leaf"));
        AppPageControl second = pageControl("right");
        second.setControlId("p2");
        assertThatCode(() -> validate(page("parent", pageControl("left"), second))).doesNotThrowAnyException();
    }

    @Test
    void refusesAPageControlNamingNoPage() {
        assertThatThrownBy(() -> validate(page("parent", pageControl(null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names no page");
    }

    @Test
    void refusesAChildPageNotInTheCatalog() {
        assertThatThrownBy(() -> validate(page("parent", pageControl("missing"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not in this catalog");
    }

    @Test
    void refusesThePageRunningItself() {
        stored("parent");
        assertThatThrownBy(() -> validate(page("parent", pageControl("parent"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same page");
    }

    @Test
    void refusesAChildThatLeadsBackToTheParent() {
        stored("middle", pageControl("parent"));
        stored("child", pageControl("middle"));
        assertThatThrownBy(() -> validate(page("parent", pageControl("child"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leads back")
                .hasMessageContaining("parent → child → middle → parent");
    }

    @Test
    void refusesAChildPageNameOnAnythingButAPageControl() {
        stored("child");
        AppPageControl grid = pageControl("child");
        grid.setType("grid");
        assertThatThrownBy(() -> validate(page("parent", grid)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a page control");
    }
}
