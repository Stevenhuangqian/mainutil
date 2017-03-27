package com.efun.encrypt.filter.algorithm;


import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AESUtils
 *
 * @author Galen
 * @since 2017/3/25
 */
public class AESUtils {

    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";

    // 加密
    public static byte[] encrypt(byte[] srcData, byte[] key, byte[] iv) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(iv));
        return cipher.doFinal(srcData);
    }

    // 解密
    public static byte[] decrypt(byte[] encData, byte[] key, byte[] iv) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(iv));
        return cipher.doFinal(encData);
    }

}
