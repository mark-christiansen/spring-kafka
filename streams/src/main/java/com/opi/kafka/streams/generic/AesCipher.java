package com.opi.kafka.streams.generic;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

public class AesCipher {

    private static final String TRANSFORMATION = "AES/ECB/PKCS5Padding";

    private Cipher cipher;
    private SecretKeySpec key;

    public AesCipher(String secret) {
        try {
            this.cipher = Cipher.getInstance(TRANSFORMATION);
            this.key = getKey(secret);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred trying to create AES cipher", e);
        }
    }

    public String encrypt(String value, String secret) {
        try {
            SecretKeySpec key = getKey(secret);
            cipher.init(Cipher.ENCRYPT_MODE, key);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred initializing cipher to encrypt", e);
        }
        return encrypt(value);
    }

    public String encrypt(String value) {
        try {
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return Base64.getEncoder().encodeToString(cipher.doFinal(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new RuntimeException("Error occurred trying to encrypt value", e);
        }
    }

    public String decrypt(String value, String secret) {
        try {
            SecretKeySpec key = getKey(secret);
            cipher.init(Cipher.DECRYPT_MODE, key);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred initializing cipher to decrypt", e);
        }
        return decrypt(value);
    }

    public String decrypt(String value) {
        try {
            cipher.init(Cipher.DECRYPT_MODE, key);
            return new String(cipher.doFinal(Base64.getDecoder().decode(value)));
        } catch (Exception e) {
            throw new RuntimeException("Error occurred trying to decrypt value", e);
        }
    }

    private SecretKeySpec getKey(String keyStr) throws NoSuchAlgorithmException {
        byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);
        key = MessageDigest.getInstance("SHA-1").digest(key);
        key = Arrays.copyOf(key, 16);
        return new SecretKeySpec(key, "AES");
    }
}
