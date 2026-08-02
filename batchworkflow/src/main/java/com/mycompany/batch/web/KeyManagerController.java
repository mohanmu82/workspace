package com.mycompany.batch.web;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.Security;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/keymanager")
public class KeyManagerController {

    static {
        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    // -------------------------------------------------------------------------
    // POST /keymanager/generate — generate a key pair + self-signed certificate.
    // Everything (PKCS12 keystore bytes + standalone public key PEM) comes back
    // base64/text in one response so the browser can offer both downloads
    // without a second round trip generating a different key pair.
    // -------------------------------------------------------------------------

    @PostMapping("/generate")
    public ResponseEntity<Map<String, Object>> generate(@RequestBody Map<String, Object> req) {
        try {
            String alias         = str(req, "alias", "mykey").trim();
            String algorithm     = str(req, "algorithm", "RSA").trim().toUpperCase();
            String keySize        = str(req, "keySize", "2048").trim();
            String curve          = str(req, "curve", "secp256r1").trim();
            String commonName    = str(req, "commonName", alias).trim();
            String organization   = str(req, "organization", "").trim();
            String orgUnit        = str(req, "organizationUnit", "").trim();
            String locality        = str(req, "locality", "").trim();
            String state           = str(req, "state", "").trim();
            String country          = str(req, "country", "").trim();
            List<String> sanEntries = parseSanEntries(str(req, "subjectAltNames", ""));
            int validityDays       = intVal(req, "validityDays", 365);
            String keystoreType    = str(req, "keystoreType", "PKCS12").trim().toUpperCase();
            String keystorePassword = str(req, "keystorePassword", "");
            String keyPassword      = str(req, "keyPassword", "");

            if (alias.isBlank()) return badRequest("Alias is required.");
            if (keystorePassword.isBlank()) return badRequest("Keystore password is required.");
            if ("PKCS12".equals(keystoreType) && keystorePassword.length() < 6)
                return badRequest("PKCS12 keystore passwords must be at least 6 characters.");
            if (keyPassword.isBlank()) keyPassword = keystorePassword;
            if (validityDays <= 0) validityDays = 365;
            if (!"RSA".equals(algorithm) && !"EC".equals(algorithm)) algorithm = "RSA";

            KeyPair keyPair = generateKeyPair(algorithm, keySize, curve);
            X500Name subject = buildDn(commonName, organization, orgUnit, locality, state, country);

            Instant notBefore = Instant.now();
            Instant notAfter  = notBefore.plus(validityDays, ChronoUnit.DAYS);
            BigInteger serial = new BigInteger(64, new SecureRandom());

            String sigAlg = "EC".equals(algorithm) ? "SHA256withECDSA" : "SHA256withRSA";
            ContentSigner signer = new JcaContentSignerBuilder(sigAlg)
                    .setProvider("BC")
                    .build(keyPair.getPrivate());

            JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                    subject, serial, Date.from(notBefore), Date.from(notAfter), subject, keyPair.getPublic());
            if (!sanEntries.isEmpty()) {
                certBuilder.addExtension(Extension.subjectAlternativeName, false, buildSanGeneralNames(sanEntries));
            }
            X509CertificateHolder certHolder = certBuilder.build(signer);
            X509Certificate cert = new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder);

            KeyStore ks = KeyStore.getInstance(keystoreType);
            ks.load(null, null);
            ks.setKeyEntry(alias, keyPair.getPrivate(), keyPassword.toCharArray(), new Certificate[]{cert});
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ks.store(bos, keystorePassword.toCharArray());

            String extension = "PKCS12".equals(keystoreType) ? ".p12" : ".jks";

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("alias", alias);
            resp.put("algorithm", algorithm);
            resp.put("keySpec", "EC".equals(algorithm) ? curve : keySize + " bits");
            resp.put("subject", subject.toString());
            resp.put("subjectAltNames", sanEntries);
            resp.put("serialNumber", serial.toString(16).toUpperCase());
            resp.put("notBefore", fmt(Date.from(notBefore)));
            resp.put("notAfter", fmt(Date.from(notAfter)));
            resp.put("sha256Fingerprint", hexFp(MessageDigest.getInstance("SHA-256").digest(cert.getEncoded())));
            resp.put("keystoreType", keystoreType);
            resp.put("keystoreFileName", alias + extension);
            resp.put("keystoreBase64", Base64.getEncoder().encodeToString(bos.toByteArray()));
            resp.put("publicKeyFileName", alias + "_public.pem");
            resp.put("publicKeyPem", toPem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
            resp.put("certificateFileName", alias + "_cert.pem");
            resp.put("certificatePem", toPem("CERTIFICATE", cert.getEncoded()));
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            return badRequest("Key generation failed: " + e.getMessage());
        }
    }

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
    // Key / certificate helpers
    // -------------------------------------------------------------------------

    private KeyPair generateKeyPair(String algorithm, String keySize, String curve) throws Exception {
        if ("EC".equals(algorithm)) {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
            gen.initialize(new ECGenParameterSpec(curve));
            return gen.generateKeyPair();
        }
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(Integer.parseInt(keySize));
        return gen.generateKeyPair();
    }

    private List<String> parseSanEntries(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null || raw.isBlank()) return out;
        for (String part : raw.split(",")) {
            String t = part.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    private GeneralNames buildSanGeneralNames(List<String> entries) {
        GeneralName[] names = new GeneralName[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            String e = entries.get(i);
            names[i] = new GeneralName(isIpAddress(e) ? GeneralName.iPAddress : GeneralName.dNSName, e);
        }
        return new GeneralNames(names);
    }

    private boolean isIpAddress(String s) {
        return s.matches("^\\d{1,3}(\\.\\d{1,3}){3}$") || s.contains(":");
    }

    private X500Name buildDn(String cn, String o, String ou, String l, String st, String c) {
        X500NameBuilder b = new X500NameBuilder(BCStyle.INSTANCE);
        if (!cn.isBlank()) b.addRDN(BCStyle.CN, cn);
        if (!ou.isBlank()) b.addRDN(BCStyle.OU, ou);
        if (!o.isBlank())  b.addRDN(BCStyle.O, o);
        if (!l.isBlank())  b.addRDN(BCStyle.L, l);
        if (!st.isBlank()) b.addRDN(BCStyle.ST, st);
        if (!c.isBlank())  b.addRDN(BCStyle.C, c);
        return b.build();
    }

    private String toPem(String type, byte[] der) {
        String b64 = Base64.getEncoder().encodeToString(der);
        StringBuilder sb = new StringBuilder();
        sb.append("-----BEGIN ").append(type).append("-----\n");
        for (int i = 0; i < b64.length(); i += 64) {
            sb.append(b64, i, Math.min(i + 64, b64.length())).append('\n');
        }
        sb.append("-----END ").append(type).append("-----\n");
        return sb.toString();
    }

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

    private int intVal(Map<String, Object> req, String key, int def) {
        Object v = req.get(key);
        if (v == null) return def;
        try { return Integer.parseInt(String.valueOf(v).trim()); } catch (Exception e) { return def; }
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
