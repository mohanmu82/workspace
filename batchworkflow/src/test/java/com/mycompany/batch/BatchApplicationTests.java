package com.mycompany.batch;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The websocket container bean in {@code WebSocketConfig} is a
 * {@link org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean},
 * which reads the JSR-356 {@code ServerContainer} out of the ServletContext when it
 * initialises. A MockServletContext never has that attribute, so the context has to be
 * loaded against a real embedded container rather than the default MOCK environment.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BatchApplicationTests {

    @Test
    void contextLoads() {
    }
}
