package com.mycompany.batch.auth;

import java.util.Base64;

public class BasicAuthProvider implements HttpAuthProvider {

    private final String header;

    public BasicAuthProvider(String username, String password) {
        String credentials = username + ":" + password;
        this.header = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
    }

    @Override
    public String getAuthorizationHeader() {
        return header;
    }
}
