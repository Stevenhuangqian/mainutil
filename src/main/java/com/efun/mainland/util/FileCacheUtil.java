package com.efun.mainland.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件缓存工具类
 * 
 * @author Administrator
 *
 */
public final class FileCacheUtil {
	private static final Logger log = LoggerFactory.getLogger(FileCacheUtil.class);
	/**
	 * 文件名Map
	 */
	private static final Map<String, String> filesMap = new ConcurrentHashMap<String, String>();
	/**
	 * 所有文件name-content
	 */
	private static final Map<String, String> fileContentMap = new ConcurrentHashMap<String, String>();

	/**
	 * 获取文件fileName的内容
	 * 
	 * @param fileName
	 * @return
	 */
	public final static String getContent(String fileName) {
		String value = fileContentMap.get(fileName);
		if (value == null) {
			loadFile(fileName);
			value = fileContentMap.get(fileName);
		}
		if (value == null) {
			return "";
		}
		return value;
	}

	/**
	 * 重新加载文件
	 * 
	 * @param fileName
	 * @return
	 */
	public final static String reLoadFile(String fileName) {
		if (fileName == null || fileName.trim().length() == 0) {
			return "no";
		}
		String str = filesMap.get(fileName);
		if ("0".equals(str)) {
			return "loading";
		} else if ("1".equals(str)) {
			try {
				filesMap.put(fileName, "0");
				return loadFile(fileName);
			} finally {
				filesMap.put(fileName, "1");
			}
		} else {
			return "no";
		}
	}

	/**
	 * 是否包含此文件
	 * 
	 * @param classFile
	 * @return
	 */
	public final static boolean containFile(String classFile) {
		return filesMap.containsKey(classFile);
	}

	/**
	 * 加载文件
	 * 
	 * @param fileName
	 * @return
	 */
	private final static String loadFile(String fileName) {

		if (fileName == null || fileName.trim().length() == 0) {
			return "no";
		}

		if ("1".equals(filesMap.get(fileName))) {
			return "ok";
		}
		synchronized (FileCacheUtil.class) {
			try {
				File file = new File(fileName);
				if (!file.exists()) {
					System.out.println("not exists fileName:" + fileName);
					log.info("not exists fileName:", fileName);
				} else if (file.isFile()) {
					String str = null;
					StringBuilder strBuilder = new StringBuilder(1024);
					long length = file.length();
					if (length > 1024 * 1024) {
						System.out.println("too long fileName:" + fileName + " length:" + length);
						log.info("too long fileName:{},length:{}", fileName, length);
					} else {
						FileReader fr = new FileReader(file);
						BufferedReader br = new BufferedReader(fr);
						while ((str = br.readLine()) != null) {
							if (CommonUtil.objectIsNotNull(str)) {
								strBuilder.append(str);
							}
						}
						fr.close();
						br.close();
					}

					str = strBuilder.toString();

					fileContentMap.put(fileName, str);
					System.out.println("fileName:" + fileName + ",length:" + length + ",fileContent:" + str);
					log.info("fileName:{},length:{},fileContent:{}", fileName, length, str);

					if (filesMap.get(fileName) == null) {
						System.out.println("加载文件成功>>>" + fileName);
						log.info("加载文件成功>>>" + fileName);
					} else {
						System.out.println("重新加载文件成功[reload]>>>classpath:" + fileName);
						log.info("重新加载文件成功[reload]>>>classpath:" + fileName);
					}
				} else {
					System.out.println("is not file fileName:" + fileName);
					log.info("is not file fileName:", fileName);
				}
			} catch (Throwable e) {
				log.error(fileName + " load fail:error", e);
				System.out.println(fileName + " load fail:error>" + e.getMessage());
			}
			// 文件加载标识
			filesMap.put(fileName, "1");
		}
		return "yes";
	}
}
