package com.mycompany.batch.auth;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class KerberosAuthProvider implements HttpAuthProvider {

    /** SPNEGO OID */
    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";

    private final Subject subject;
    private final String servicePrincipal;

    /**
     * @param username         Kerberos principal (e.g. {@code user@REALM.COM})
     * @param keytabPath       Absolute path to the keytab file
     * @param servicePrincipal Target service principal (e.g. {@code HTTP/host.realm.com@REALM.COM})
     */
    public KerberosAuthProvider(String username, String keytabPath, String servicePrincipal) throws Exception {
        this.servicePrincipal = servicePrincipal;
        this.subject = login(username, keytabPath);
    }

    @Override
    public String getAuthorizationHeader() throws Exception {
        byte[] token = Subject.callAs(subject, () -> {
            GSSManager manager = GSSManager.getInstance();
            Oid spnegoOid = new Oid(SPNEGO_OID);
            GSSName serverName = manager.createName(servicePrincipal, GSSName.NT_HOSTBASED_SERVICE);
            GSSContext context = manager.createContext(serverName, spnegoOid, null, GSSContext.DEFAULT_LIFETIME);
            context.requestMutualAuth(true);
            return context.initSecContext(new byte[0], 0, 0);
        });
        return "Negotiate " + Base64.getEncoder().encodeToString(token);
    }

    private static Subject login(String username, String keytabPath) throws Exception {
        Configuration config = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, Object> options = new HashMap<>();
                options.put("principal", username);
                options.put("keyTab", keytabPath);
                options.put("useKeyTab", "true");
                options.put("storeKey", "true");
                options.put("isInitiator", "true");
                options.put("refreshKrb5Config", "true");
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                "com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)
                };
            }
        };
        LoginContext lc = new LoginContext("batch-kerberos", null, null, config);
        lc.login();
        return lc.getSubject();
    }
}
