package com.mycompany.taskmanagement.config;

import org.apache.catalina.connector.Connector;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Adds a plain-HTTP connector on server.http.port and a security constraint requiring
 * confidential (HTTPS) transport for every URL, so Tomcat 301-redirects all HTTP traffic
 * to the HTTPS connector on server.port instead of serving it directly.
 */
@Configuration
public class HttpsRedirectConfig {

    @Value("${server.http.port}")
    private int httpPort;

    @Value("${server.port}")
    private int httpsPort;

    @Bean
    public WebServerFactoryCustomizer<TomcatServletWebServerFactory> httpToHttpsRedirectConnector() {
        return factory -> {
            Connector redirectConnector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
            redirectConnector.setScheme("http");
            redirectConnector.setPort(httpPort);
            redirectConnector.setSecure(false);
            redirectConnector.setRedirectPort(httpsPort);
            factory.addAdditionalTomcatConnectors(redirectConnector);

            factory.addContextCustomizers(context -> {
                SecurityConstraint constraint = new SecurityConstraint();
                constraint.setUserConstraint("CONFIDENTIAL");
                SecurityCollection collection = new SecurityCollection();
                collection.addPattern("/*");
                constraint.addCollection(collection);
                context.addConstraint(constraint);
            });
        };
    }
}
