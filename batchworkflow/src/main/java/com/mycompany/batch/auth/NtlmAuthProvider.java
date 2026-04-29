package com.mycompany.batch.auth;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

public class NtlmAuthProvider implements HttpAuthProvider {

    private final String username;
    private final String password;

    public NtlmAuthProvider(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /** Returns null — NTLM is a challenge-response protocol handled via {@link #toAuthenticator()}. */
    @Override
    public String getAuthorizationHeader() {
        return null;
    }

    /** Returns an {@link Authenticator} that supplies credentials for the NTLM handshake. */
    public Authenticator toAuthenticator() {
        return new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password.toCharArray());
            }
        };
    }

    public String getUsername() { return username; }
    public String getPassword() { return password; }
}
