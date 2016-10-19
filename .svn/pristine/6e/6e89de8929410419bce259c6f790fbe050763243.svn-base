package com.efun.mainland.util;

import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;

public class SecretUtil {

	private static final String IV_3DES = "efun2015";// 长度必须是8
	private static SecretKeyFactory KEY_FACTORY;
	private static IvParameterSpec IPS;
	private static final String DEFAULT_SECRET_KEY = "com.efun_20150311_secret";// 长度必须是24
	
	static {
		try {
			KEY_FACTORY = SecretKeyFactory.getInstance("desede");
			IPS = new IvParameterSpec(IV_3DES.getBytes("UTF-8"));
		} catch (Exception e) {
			e.printStackTrace();
			KEY_FACTORY = null;
			IPS = new IvParameterSpec(IV_3DES.getBytes());
		}
	}

	/**
	 * 加密
	 * 
	 * @param plainText
	 * @param secretKey
	 * @return
	 * @throws Exception
	 */
	public static final String encrypt(String plainText, String secretKey)
			throws Exception {
		if (secretKey == null || secretKey.length() != 24) {
			secretKey = DEFAULT_SECRET_KEY;
		}
		DESedeKeySpec spec = new DESedeKeySpec(secretKey.getBytes("UTF-8"));
		Key deskey = KEY_FACTORY.generateSecret(spec);
		Cipher cipher = Cipher.getInstance("desede/CBC/PKCS5Padding");
		cipher.init(Cipher.ENCRYPT_MODE, deskey, IPS);
		byte[] encryptData = cipher.doFinal(plainText.getBytes("UTF-8"));
		String base64_ciphertext = Base64Util.encode(encryptData);
		return base64_ciphertext;
	}

	/**
	 * 解密
	 * 
	 * @param encryptText
	 * @param secretKey
	 * @return
	 * @throws Exception
	 */
	public static final String decrypt(String encryptText, String secretKey)
			throws Exception {
		if (secretKey == null || secretKey.length() != 24) {
			secretKey = DEFAULT_SECRET_KEY;
		}
		DESedeKeySpec spec = new DESedeKeySpec(secretKey.getBytes("utf-8"));
		Key deskey = KEY_FACTORY.generateSecret(spec);
		Cipher cipher = Cipher.getInstance("desede/CBC/PKCS5Padding");
		cipher.init(Cipher.DECRYPT_MODE, deskey, IPS);
		byte[] decryptData = cipher.doFinal(Base64Util.decode(encryptText
				.getBytes("UTF-8")));
		return new String(decryptData, "UTF-8");
	}
	
}
