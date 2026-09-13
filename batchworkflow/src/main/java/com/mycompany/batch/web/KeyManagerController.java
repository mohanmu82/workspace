package com.mycompany.batch.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/keymanager")
public class KeyManagerController {

    // -------------------------------------------------------------------------
    // POST /keymanager/list — open a keystore file on the server and list its
    // entries (alias, entry type, and certificate details where present). Never
    // returns private key material.
    // -------------------------------------------------------------------------

    @PostMapping("/list")
    public ResponseEntity<Map<String, Object>> list(@RequestBody Map<String, Object> req) {
        String path     = str(req, "path", "").trim();
        String password = str(req, "password", "");
        String type     = str(req, "type", "PKCS12").trim().toUpperCase();

        if (path.isBlank()) return badRequest("Keystore path is required.");
        File file = new File(path);
        if (!file.isFile()) return badRequest("File not found: " + path);

        List<Map<String, Object>> entries = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file)) {
            KeyStore ks = KeyStore.getInstance(type);
            ks.load(fis, password.isBlank() ? null : password.toCharArray());

            Enumeration<String> aliases = ks.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                entries.add(entryToMap(ks, alias));
            }
        } catch (Exception e) {
            return badRequest("Could not open keystore: " + e.getMessage());
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("path", path);
        resp.put("keystoreType", type);
        resp.put("data", entries);
        return ResponseEntity.ok(resp);
    }

    private Map<String, Object> entryToMap(KeyStore ks, String alias) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("alias", alias);

        String entryType;
        Certificate[] chain = ks.isKeyEntry(alias) ? ks.getCertificateChain(alias) : null;
        if (ks.isCertificateEntry(alias)) entryType = "TrustedCertEntry";
        else if (chain != null && chain.length > 0) entryType = "PrivateKeyEntry";
        else if (ks.isKeyEntry(alias)) entryType = "SecretKeyEntry";
        else entryType = "Unknown";
        m.put("entryType", entryType);
        m.put("chainLength", chain != null ? chain.length : 0);

        try {
            Date created = ks.getCreationDate(alias);
            if (created != null) m.put("creationDate", fmt(created));
        } catch (Exception ignored) {}

        Certificate cert = ks.getCertificate(alias);
        if (cert instanceof X509Certificate x509) {
            m.putAll(certToMap(x509));
        }

        return m;
    }

    // -------------------------------------------------------------------------
    // Certificate helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> certToMap(X509Certificate cert) {
        Map<String, Object> m = new LinkedHashMap<>();
        String subjectDN = cert.getSubjectX500Principal().getName();
        String issuerDN  = cert.getIssuerX500Principal().getName();

        m.put("subject",   subjectDN);
        m.put("subjectCN", rdn(subjectDN, "CN"));
        m.put("issuer",    issuerDN);
        m.put("issuerCN",  rdn(issuerDN, "CN"));
        m.put("validFrom", fmt(cert.getNotBefore()));
        m.put("validTo",   fmt(cert.getNotAfter()));
        m.put("isExpired", cert.getNotAfter().before(new Date()));
        m.put("serialNumber", cert.getSerialNumber().toString(16).toUpperCase());
        m.put("sigAlg",    cert.getSigAlgName());
        m.put("keyAlgorithm", cert.getPublicKey().getAlgorithm());
        m.put("keyBits",   keyBits(cert));
        m.put("isSelfSigned", cert.getSubjectX500Principal().equals(cert.getIssuerX500Principal()));

        try {
            m.put("sha256Fingerprint", hexFp(MessageDigest.getInstance("SHA-256").digest(cert.getEncoded())));
        } catch (Exception ignored) {}

        m.put("subjectAltNames", subjectAltNames(cert));

        return m;
    }

    private List<String> subjectAltNames(X509Certificate cert) {
        List<String> out = new ArrayList<>();
        try {
            Collection<List<?>> sans = cert.getSubjectAlternativeNames();
            if (sans != null) {
                for (List<?> san : sans) out.add(String.valueOf(san.get(1)));
            }
        } catch (Exception ignored) {}
        return out;
    }

    private int keyBits(X509Certificate cert) {
        try {
            java.security.PublicKey pk = cert.getPublicKey();
            if (pk instanceof java.security.interfaces.RSAPublicKey r) return r.getModulus().bitLength();
            if (pk instanceof java.security.interfaces.ECPublicKey  e) return e.getParams().getOrder().bitLength();
        } catch (Exception ignored) {}
        return 0;
    }

    private String rdn(String dn, String type) {
        for (String part : dn.split(",")) {
            part = part.trim();
            if (part.startsWith(type + "=")) return part.substring(type.length() + 1);
        }
        return "";
    }

    private String hexFp(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) { if (!sb.isEmpty()) sb.append(':'); sb.append(String.format("%02X", x)); }
        return sb.toString();
    }

    private String fmt(Date d) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").format(d);
    }

    // -------------------------------------------------------------------------
    // Request helpers
    // -------------------------------------------------------------------------

    private String str(Map<String, Object> req, String key, String def) {
        Object v = req.get(key);
        return v != null ? String.valueOf(v) : def;
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
