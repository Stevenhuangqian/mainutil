package com.efun.encrypt.filter.algorithm;


import javax.crypto.Cipher;

import org.apache.log4j.Logger;

import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * RSAUtils
 *
 * @author Galen
 * @since 2017/3/25
 */
public class RSAUtils {

    private static final Logger logger = Logger.getLogger(RSAUtils.class);

    static RSAPrivateKey privateKey;
    static RSAPublicKey publicKey;

    private static final byte[] RSA_PUBLIC_KEY_B = new byte[]{77, 73, 73, 66, 73, 106, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81, 69, 70, 65, 65, 79, 67, 65, 81, 56, 65, 77, 73, 73, 66, 67, 103, 75, 67, 65, 81, 69, 65, 122, 118, 75, 87, 98, 81, 50, 66, 55, 103, 88, 116, 84, 81, 70, 51, 70, 54, 118, 105, 121, 50, 54, 99, 83, 115, 111, 74, 48, 50, 90, 98, 113, 109, 117, 100, 82, 84, 88, 84, 68, 81, 108, 118, 113, 78, 105, 86, 121, 50, 55, 105, 87, 107, 53, 106, 98, 74, 77, 80, 57, 43, 110, 73, 73, 84, 115, 73, 68, 85, 114, 53, 110, 71, 56, 57, 75, 119, 117, 74, 117, 73, 75, 112, 112, 52, 81, 105, 81, 69, 112, 100, 101, 115, 55, 102, 74, 97, 49, 88, 75, 118, 113, 54, 110, 81, 104, 122, 66, 57, 48, 78, 110, 103, 116, 105, 57, 120, 86, 70, 78, 55, 76, 67, 79, 51, 52, 48, 86, 43, 102, 110, 54, 79, 66, 69, 66, 51, 89, 90, 89, 66, 89, 69, 88, 111, 115, 54, 98, 109, 121, 118, 108, 78, 72, 97, 70, 99, 121, 52, 77, 55, 83, 56, 52, 55, 70, 55, 112, 55, 112, 82, 65, 51, 85, 67, 87, 73, 101, 43, 100, 104, 71, 104, 122, 43, 55, 120, 85, 71, 49, 67, 107, 110, 88, 103, 121, 54, 114, 119, 82, 78, 65, 115, 78, 99, 117, 51, 107, 87, 98, 105, 98, 56, 75, 115, 80, 55, 57, 43, 81, 71, 86, 108, 78, 100, 75, 100, 54, 68, 72, 82, 107, 86, 75, 57, 47, 53, 84, 53, 67, 110, 72, 70, 114, 73, 113, 90, 76, 117, 67, 82, 86, 117, 80, 119, 76, 112, 70, 49, 83, 75, 53, 55, 77, 97, 66, 56, 101, 98, 117, 87, 54, 103, 111, 76, 56, 67, 53, 80, 51, 101, 54, 77, 104, 52, 88, 81, 84, 87, 72, 113, 65, 49, 119, 87, 120, 65, 113, 74, 113, 111, 85, 80, 107, 65, 100, 87, 67, 115, 79, 110, 98, 84, 87, 103, 53, 54, 76, 83, 121, 107, 90, 54, 99, 72, 116, 112, 54, 99, 65, 70, 119, 99, 49, 100, 43, 68, 78, 101, 50, 119, 73, 68, 65, 81, 65, 66};
    private static final byte[] RSA_PRIVATE_KEY_B = new byte[]{77, 73, 73, 69, 118, 81, 73, 66, 65, 68, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81, 69, 70, 65, 65, 83, 67, 66, 75, 99, 119, 103, 103, 83, 106, 65, 103, 69, 65, 65, 111, 73, 66, 65, 81, 68, 79, 56, 112, 90, 116, 68, 89, 72, 117, 66, 101, 49, 78, 65, 88, 99, 88, 113, 43, 76, 76, 98, 112, 120, 75, 121, 103, 110, 84, 90, 108, 117, 113, 97, 53, 49, 70, 78, 100, 77, 78, 67, 87, 43, 111, 50, 74, 88, 76, 98, 117, 74, 97, 84, 109, 78, 115, 107, 119, 47, 51, 54, 99, 103, 104, 79, 119, 103, 78, 83, 118, 109, 99, 98, 122, 48, 114, 67, 52, 109, 52, 103, 113, 109, 110, 104, 67, 74, 65, 83, 108, 49, 54, 122, 116, 56, 108, 114, 86, 99, 113, 43, 114, 113, 100, 67, 72, 77, 72, 51, 81, 50, 101, 67, 50, 76, 51, 70, 85, 85, 51, 115, 115, 73, 55, 102, 106, 82, 88, 53, 43, 102, 111, 52, 69, 81, 72, 100, 104, 108, 103, 70, 103, 82, 101, 105, 122, 112, 117, 98, 75, 43, 85, 48, 100, 111, 86, 122, 76, 103, 122, 116, 76, 122, 106, 115, 88, 117, 110, 117, 108, 69, 68, 100, 81, 74, 89, 104, 55, 53, 50, 69, 97, 72, 80, 55, 118, 70, 81, 98, 85, 75, 83, 100, 101, 68, 76, 113, 118, 66, 69, 48, 67, 119, 49, 121, 55, 101, 82, 90, 117, 74, 118, 119, 113, 119, 47, 118, 51, 53, 65, 90, 87, 85, 49, 48, 112, 51, 111, 77, 100, 71, 82, 85, 114, 51, 47, 108, 80, 107, 75, 99, 99, 87, 115, 105, 112, 107, 117, 52, 74, 70, 87, 52, 47, 65, 117, 107, 88, 86, 73, 114, 110, 115, 120, 111, 72, 120, 53, 117, 53, 98, 113, 67, 103, 118, 119, 76, 107, 47, 100, 55, 111, 121, 72, 104, 100, 66, 78, 89, 101, 111, 68, 88, 66, 98, 69, 67, 111, 109, 113, 104, 81, 43, 81, 66, 49, 89, 75, 119, 54, 100, 116, 78, 97, 68, 110, 111, 116, 76, 75, 82, 110, 112, 119, 101, 50, 110, 112, 119, 65, 88, 66, 122, 86, 51, 52, 77, 49, 55, 98, 65, 103, 77, 66, 65, 65, 69, 67, 103, 103, 69, 65, 80, 97, 117, 90, 72, 69, 74, 115, 78, 56, 120, 89, 88, 82, 120, 85, 113, 120, 56, 122, 53, 76, 43, 47, 110, 54, 72, 53, 114, 53, 47, 80, 76, 85, 103, 73, 103, 47, 108, 52, 80, 87, 104, 85, 101, 66, 75, 106, 65, 81, 54, 52, 77, 89, 85, 117, 112, 56, 112, 106, 100, 100, 79, 98, 75, 104, 118, 51, 87, 69, 55, 66, 100, 57, 98, 71, 101, 97, 57, 107, 105, 84, 71, 87, 56, 83, 54, 49, 76, 107, 100, 54, 57, 47, 47, 121, 67, 55, 53, 79, 80, 97, 97, 101, 79, 102, 71, 115, 112, 101, 108, 65, 66, 53, 115, 74, 69, 79, 121, 114, 120, 100, 57, 108, 100, 109, 107, 122, 110, 65, 97, 108, 50, 52, 89, 74, 102, 57, 101, 51, 49, 67, 82, 55, 104, 78, 107, 107, 118, 111, 100, 120, 65, 51, 53, 78, 54, 84, 85, 113, 112, 50, 51, 121, 53, 68, 43, 100, 119, 43, 116, 43, 67, 112, 48, 120, 75, 121, 50, 117, 101, 85, 79, 78, 103, 54, 116, 99, 103, 55, 115, 87, 118, 52, 82, 115, 120, 120, 68, 100, 73, 99, 57, 71, 106, 51, 70, 83, 113, 104, 107, 111, 72, 75, 100, 54, 77, 43, 113, 79, 103, 109, 118, 78, 86, 56, 114, 74, 119, 121, 87, 56, 56, 71, 111, 90, 89, 43, 85, 80, 82, 81, 112, 102, 65, 117, 117, 75, 110, 48, 79, 103, 84, 103, 100, 56, 49, 108, 83, 107, 122, 83, 118, 52, 101, 66, 53, 109, 55, 47, 69, 103, 70, 71, 54, 50, 98, 100, 117, 83, 51, 67, 116, 57, 103, 109, 101, 72, 51, 53, 109, 90, 112, 79, 106, 85, 122, 90, 52, 99, 66, 83, 117, 51, 104, 110, 76, 120, 81, 69, 76, 48, 73, 73, 113, 103, 47, 67, 111, 117, 78, 79, 70, 113, 106, 112, 50, 87, 72, 69, 51, 121, 87, 111, 81, 56, 104, 98, 87, 119, 73, 81, 75, 66, 103, 81, 68, 47, 119, 110, 69, 87, 53, 73, 47, 72, 110, 73, 52, 79, 102, 100, 55, 80, 72, 104, 110, 83, 118, 119, 73, 53, 111, 85, 113, 54, 49, 111, 48, 90, 99, 122, 72, 110, 99, 72, 83, 74, 104, 74, 116, 74, 82, 75, 72, 114, 100, 76, 81, 68, 122, 103, 110, 115, 76, 47, 104, 87, 56, 88, 121, 121, 54, 113, 89, 50, 121, 115, 118, 57, 65, 85, 90, 89, 65, 73, 82, 74, 77, 103, 108, 53, 84, 50, 122, 108, 107, 72, 52, 98, 116, 105, 70, 43, 121, 106, 53, 102, 74, 43, 69, 109, 69, 73, 118, 55, 117, 56, 79, 117, 49, 74, 83, 65, 70, 121, 71, 103, 99, 115, 57, 55, 66, 72, 67, 52, 113, 50, 71, 108, 85, 76, 89, 71, 71, 50, 99, 100, 107, 86, 115, 73, 86, 86, 70, 76, 78, 73, 104, 114, 71, 72, 101, 97, 43, 120, 69, 71, 105, 48, 101, 79, 120, 67, 108, 50, 54, 81, 75, 66, 103, 81, 68, 80, 74, 71, 87, 56, 71, 104, 97, 52, 89, 47, 121, 111, 82, 119, 79, 55, 73, 72, 112, 99, 104, 101, 79, 107, 54, 117, 108, 81, 56, 117, 74, 65, 111, 80, 53, 77, 80, 107, 73, 121, 52, 97, 116, 69, 118, 69, 113, 50, 81, 78, 74, 43, 108, 116, 99, 82, 66, 66, 76, 43, 72, 48, 106, 54, 74, 88, 70, 103, 49, 65, 43, 47, 111, 106, 50, 48, 52, 50, 102, 76, 50, 54, 107, 113, 115, 75, 115, 106, 48, 81, 103, 99, 71, 120, 49, 70, 69, 84, 43, 66, 80, 51, 90, 85, 78, 97, 89, 108, 107, 55, 115, 99, 98, 51, 109, 98, 97, 84, 106, 97, 70, 108, 76, 98, 114, 118, 53, 57, 113, 107, 102, 69, 103, 100, 74, 97, 67, 101, 79, 48, 71, 88, 112, 99, 54, 50, 108, 104, 111, 117, 104, 111, 65, 90, 81, 101, 83, 106, 81, 67, 79, 77, 85, 65, 115, 121, 56, 86, 73, 119, 75, 66, 103, 81, 68, 106, 49, 69, 89, 66, 97, 113, 70, 90, 52, 50, 47, 52, 73, 78, 65, 50, 71, 69, 53, 81, 109, 97, 53, 86, 119, 65, 82, 100, 52, 51, 110, 86, 122, 102, 81, 75, 103, 118, 97, 77, 43, 102, 43, 75, 105, 81, 84, 107, 82, 104, 87, 70, 118, 105, 106, 65, 113, 81, 114, 69, 67, 71, 74, 89, 122, 119, 89, 53, 107, 80, 87, 100, 55, 50, 68, 71, 83, 56, 43, 76, 108, 72, 77, 48, 67, 84, 105, 115, 86, 115, 97, 47, 48, 77, 75, 78, 117, 52, 78, 77, 75, 75, 52, 55, 120, 107, 109, 115, 101, 86, 113, 98, 104, 117, 113, 121, 72, 43, 106, 111, 107, 78, 50, 97, 98, 66, 52, 116, 111, 120, 49, 99, 115, 107, 85, 122, 115, 51, 49, 114, 114, 87, 119, 50, 82, 67, 111, 105, 111, 67, 49, 86, 109, 88, 111, 83, 75, 77, 122, 70, 48, 53, 82, 117, 55, 80, 99, 121, 52, 81, 75, 66, 103, 68, 108, 102, 99, 50, 52, 117, 75, 54, 122, 87, 120, 78, 119, 51, 68, 121, 108, 57, 84, 87, 98, 106, 68, 114, 74, 87, 47, 108, 105, 86, 98, 87, 86, 74, 77, 105, 89, 98, 43, 71, 110, 118, 90, 50, 105, 97, 88, 110, 99, 115, 98, 83, 68, 121, 49, 111, 54, 51, 43, 105, 113, 52, 114, 53, 88, 90, 87, 82, 88, 103, 112, 112, 119, 70, 97, 43, 72, 50, 79, 111, 71, 85, 84, 98, 54, 116, 81, 68, 109, 106, 90, 65, 79, 107, 51, 117, 54, 75, 89, 72, 83, 69, 66, 49, 111, 55, 111, 100, 69, 50, 111, 100, 83, 50, 97, 53, 66, 73, 68, 74, 56, 98, 72, 56, 103, 74, 75, 67, 80, 87, 84, 104, 112, 51, 101, 114, 80, 70, 109, 88, 81, 65, 117, 111, 117, 83, 67, 74, 89, 113, 99, 81, 83, 100, 113, 49, 68, 89, 98, 74, 57, 81, 54, 102, 47, 80, 111, 88, 76, 82, 65, 111, 71, 65, 84, 69, 121, 86, 109, 83, 114, 114, 50, 49, 77, 83, 122, 82, 48, 114, 89, 43, 99, 51, 43, 76, 84, 77, 82, 70, 67, 51, 67, 105, 102, 75, 53, 112, 47, 66, 120, 102, 80, 81, 114, 51, 88, 113, 110, 97, 55, 105, 57, 119, 90, 75, 73, 108, 113, 67, 80, 73, 53, 116, 53, 68, 87, 114, 84, 89, 116, 117, 80, 43, 106, 122, 114, 51, 52, 75, 88, 68, 103, 117, 49, 74, 79, 65, 49, 53, 115, 107, 51, 80, 89, 111, 71, 65, 48, 120, 86, 54, 122, 55, 82, 98, 83, 47, 115, 53, 111, 57, 65, 71, 49, 49, 107, 81, 49, 53, 72, 72, 77, 52, 81, 82, 55, 79, 101, 85, 51, 55, 79, 48, 49, 85, 115, 57, 119, 81, 105, 114, 120, 78, 47, 106, 85, 77, 114, 89, 80, 72, 119, 112, 52, 74, 99, 71, 74, 111, 76, 68, 89, 56, 66, 51, 83, 119, 51, 119, 74, 75, 74, 78, 111, 61};

    static {
        try{
            byte[] privatekeyBytes = Base64.getDecoder().decode(RSA_PRIVATE_KEY_B);
            KeyFactory keyFactory= KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privatekeyBytes);
            privateKey= (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
        } catch(Exception ex){
            logger.error(ex.getMessage(), ex);
        }

    }

    public static byte[] encrypt(byte[] srcBytes) throws Exception {
        if (privateKey != null) {
            Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.ENCRYPT_MODE, privateKey);
            byte[] resultBytes = cipher.doFinal(srcBytes);
            return resultBytes;
        }
        return null;
    }

    public static byte[] decrypt(byte[] encBytes) throws Exception {
        if (privateKey != null) {
            Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.DECRYPT_MODE, privateKey);
            byte[] decBytes = cipher.doFinal(encBytes);
            return decBytes;
        }
        return null;
    }
}