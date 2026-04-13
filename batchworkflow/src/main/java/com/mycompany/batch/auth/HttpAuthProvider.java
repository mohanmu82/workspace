package com.mycompany.batch.auth;

public interface HttpAuthProvider {

    /**
     * Returns the value for the {@code Authorization} header,
     * or {@code null} if no authentication is required.
     */
    String getAuthorizationHeader() throws Exception;
}
