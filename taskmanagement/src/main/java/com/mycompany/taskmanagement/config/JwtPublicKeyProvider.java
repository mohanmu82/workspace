package com.mycompany.taskmanagement.config;

import io.jsonwebtoken.security.Jwk;
import io.jsonwebtoken.security.JwkSet;
import io.jsonwebtoken.security.Jwks;
import io.jsonwebtoken.security.RsaPublicJwk;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.interfaces.RSAPublicKey;

/**
 * Fetches the IdP's JWKS document once at startup and caches the single RSA public key
 * matching auth.key-id, so {@link JwtAuthFilter} never has to hit the network per request.
 */
@Component
@RequiredArgsConstructor
public class JwtPublicKeyProvider {

    private final AuthProperties authProperties;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    private volatile RSAPublicKey publicKey;

    @PostConstruct
    void init() {
        if (authProperties.isEnabled()) {
            publicKey = fetchKey();
        }
    }

    public RSAPublicKey getPublicKey() {
        if (publicKey == null) {
            throw new IllegalStateException("JWT public key is not available");
        }
        return publicKey;
    }

    private RSAPublicKey fetchKey() {
        String jwkUrl = authProperties.getJwkUrl();
        String keyId = authProperties.getKeyId();
        String body;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(jwkUrl)).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Failed to fetch JWK set from " + jwkUrl + ": HTTP " + response.statusCode());
            }
            body = response.body();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to fetch JWK set from " + jwkUrl, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while fetching JWK set from " + jwkUrl, e);
        }

        JwkSet jwkSet = Jwks.setParser().build().parse(body);
        for (Jwk<?> jwk : jwkSet.getKeys()) {
            if (keyId.equals(jwk.getId())) {
                if (!(jwk instanceof RsaPublicJwk rsaJwk)) {
                    throw new IllegalStateException(
                            "Key '" + keyId + "' at " + jwkUrl + " is not an RSA public key");
                }
                return rsaJwk.toKey();
            }
        }
        throw new IllegalStateException("No key with kid '" + keyId + "' found in JWK set at " + jwkUrl);
    }
}
