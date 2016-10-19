package com.efun.mainland.util;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.efun.mainland.util.zookeeper.ZKCurator;


/**
 * 属性文件自动加载器
 * 
 * @author Administrator
 *
 */
public class PropertiesFileLoader {
	/**
	 * web工程根路径
	 */
	private static final String WEB_ROOT;

	/**
	 * web工程下的WEB-INF路径
	 */
	private static final String WEB_INF_PATH;

	/**
	 * web工程下的classpath路径
	 */
	private static final String CLASS_PATH;

	/**
	 * 取当前操作系统版本名
	 */
	private static final String SYSTEM_NAME = System.getProperty("os.name");

	private static final boolean IS_WINDOWNS = (SYSTEM_NAME != null && (SYSTEM_NAME.toLowerCase().contains("windows")));

	/**
	 * 加载状态值
	 */
	private static final AtomicBoolean IS_LOAD = new AtomicBoolean(false);

	/**
	 * 默认间隔时间为5s
	 */
	private String intervalTime = "10";

	/**
	 * 是否加载所有文件
	 */
	private boolean reloadAll = false;
	/**
	 * 文件类型
	 */
	private List<String> fileTypeList = new ArrayList<String>();
	/**
	 * 包含文件
	 */
	private List<String> includeList = new ArrayList<String>();
	/**
	 * 不包含文件
	 */
	private List<String> exclusiveList = new ArrayList<String>();
	/**
	 * 定时器
	 */
	private Timer timer = null;
	/**
	 * 加载的文件
	 */
	private Map<String, Long> filesMap = new HashMap<String, Long>();
	
	private ExecutorService executorService= Executors.newFixedThreadPool(2);

	private static final Logger logger = Logger.getLogger(PropertiesFileLoader.class);

	static {
		CLASS_PATH = loadClassPath();
		WEB_INF_PATH = chopLastSection(CLASS_PATH);
		WEB_ROOT = chopLastSection(WEB_INF_PATH);
	}
	
	public static final boolean isWindows(){
		return IS_WINDOWNS;
	}

	/**
	 * 获取web工程classpath路径，该路径以文件分隔符结束
	 *
	 * @return web工程根路径
	 */
	public static final String getClassPath() {
		return CLASS_PATH;
	}

	/**
	 * 获取web工程根路径，该路径以文件分隔符结束
	 * 
	 * @return web工程根路径
	 */
	public static final String getWebRoot() {
		return WEB_ROOT;
	}

	/**
	 * 获取web工程下的WEB-INF路径，该路径以文件分隔符结束
	 * 
	 * @return web工程下的WEB-INF路径
	 */
	public static final String getWebInfPath() {
		return WEB_INF_PATH;
	}

	private static final String chopLastSeparator(String src) {
		if (src!=null&&src.endsWith(File.separator)) {
			src = src.substring(0, src.lastIndexOf(File.separator));
		}
		return src;
	}

	private static final String chopLastSection(String src) {
		if(src==null){
			return null;
		}
		String temp = chopLastSeparator(src);
		return temp.substring(0, temp.lastIndexOf(File.separator) + 1);
	}


	private static final String getFilePath(File file,String searchStr){
		String temp=file.getAbsolutePath();
		if(temp.contains(searchStr)){
			return temp.substring(0,temp.indexOf(searchStr))+searchStr;
		}else if (file.isDirectory()){
			File[] files=file.listFiles();
			if (files!=null&&files.length>0){
				for (File tempFile:files){
					String tempResult=getFilePath(tempFile,searchStr);
					if (tempResult!=null){
						return tempResult;
					}
				}
			}
		}
		return null;
	}

	/**
	 * 获取classpath
	 * 
	 * @return classpath
	 */
	public static final String loadClassPath() {
		String url=null;
		try {
			try {
				url= getDefaultClassLoader().getResource("/").toString().replace("/", File.separator);
			} catch (Exception e) {
			}

			System.out.println("load ClassLoader init:"+url);
			logger.info("load ClassLoader init:"+url);
			if(CommonUtil.objectIsNotNull(url)){
				while(url.endsWith(File.separator)){
					url=url.substring(0, url.length()-1);
				}
			}
			
			if(CommonUtil.objectIsNull(url)||(!url.endsWith("classes"))){
				url = PropertiesFileLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath().replace("/", File.separator);
				System.out.println("load ClassLoader success: PropertiesFileLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath()");
				logger.info("load ClassLoader success: PropertiesFileLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath()");
			}

			String prefix = "file:".concat(File.separator);
			if (url.startsWith(prefix)) {
				url = url.substring(prefix.length());
				if (!IS_WINDOWNS) {
					if (url.indexOf(":") > -1) {
						url = url.substring(File.separator.length());
					}
				}
			}

			int index=url.indexOf("WEB-INF");
			if(index!=-1){
				url=url.substring(0, index);
				while(url.endsWith(File.separator)){
					url=url.substring(0, url.length()-1);
				}
				url=url+File.separator+"WEB-INF"+File.separator+"classes";
			}else{
				File tempD=new File(url).getParentFile();
				url = getFilePath(tempD,"WEB-INF"+File.separator+"classes");
			}
			
			System.out.println("load ClassLoader finish:"+url);
			logger.info("load ClassLoader finish:"+url);
			
			url = URLDecoder.decode(url, Charset.defaultCharset().toString());

			if (!IS_WINDOWNS) {
				if(!url.startsWith("/")){
					url = "/" + url;
				}
				if(!url.endsWith("/")){
					url =  url+"/";
				}
			}else{
				if(url.startsWith("\\")){
					url = url.substring(1);
				}
				if(!url.endsWith("\\")){
					url =url+ "\\" ;
				}
			}
		} catch (Exception e) {
			logger.error("The system should always have the platform default:"+e.getMessage(),e);
			System.out.println("The system should always have the platform default:"+e.getMessage());
		}finally{
			System.out.println("classpath:"+url);
			logger.info("classpath:"+url);
		}
		return url;
	}

	public String getIntervalTime() {
		return intervalTime;
	}

	public void setIntervalTime(String intervalTime) {
		if(CommonUtil.isNumeric(intervalTime)){
			this.intervalTime = intervalTime;
		}
	}

	public List<String> getFileTypeList() {
		return fileTypeList;
	}

	public void setFileTypeList(List<String> fileTypeList) {
		if(fileTypeList!=null){
			this.fileTypeList = fileTypeList;
		}
	}

	public List<String> getIncludeList() {
		return includeList;
	}

	public void setIncludeList(List<String> includeList) {
		if(includeList!=null){
			this.includeList = includeList;
		}
	}

	public List<String> getExclusiveList() {
		return exclusiveList;
	}

	public void setExclusiveList(List<String> exclusiveList) {
		if(exclusiveList!=null){
			this.exclusiveList = exclusiveList;
		}
	}

	private static final String getFileSuffix(String fileName) {
		int index = fileName.lastIndexOf(".");
		if (index != -1) {
			return fileName.substring(index + 1);
		} else {
			return "";
		}
	}

	private static final String getRelativeClassPath(String filePath) {
		if (filePath.startsWith(CLASS_PATH)) {
			return filePath.substring(CLASS_PATH.length());
		} else {
			return filePath;
		}
	}
	
	private boolean isInclude(String filePath){
		if(CommonUtil.objectIsNotNull(filePath)&&includeList.size()>0){
			for(String str:includeList){
				if(CommonUtil.objectIsNotNull(str)&&filePath.startsWith(str)){
					return true;
				}
			}
		}
		return false;
	}

	private synchronized void reloadFile(File file,boolean relativeClassPath) {
		if (file.canRead()) {
			if (file.isFile()) {
				String filePath = file.getAbsolutePath();
				String tempPath = getRelativeClassPath(filePath);
				if ((reloadAll
						|| fileTypeList.contains(getFileSuffix(filePath)) || 
						isInclude(filePath))
						&& !(relativeClassPath?exclusiveList.contains(tempPath):exclusiveList.contains(filePath))) {
					Long lastTime = filesMap.get(filePath);
					
					if (lastTime == null
							|| lastTime.longValue() != file.lastModified()) {
						filesMap.put(filePath, file.lastModified());
						if(relativeClassPath&&filePath.endsWith(".properties")){
							if (lastTime == null
									&& PropertiesCacheUtil.containFile(tempPath)) {
								System.out.println("load file [classpath]:{"
										+ tempPath + "} result: yes");
								logger.info("load file [classpath]:{" + tempPath
										+ "} result:yes ");
							} else {
								String reslut = PropertiesCacheUtil
										.reLoadPropertiesFile(tempPath);
								System.out.println("reload file [classpath]:{"
										+ tempPath + "} result: " + reslut);
								logger.info("reload file [classpath]:{" + tempPath
										+ "} result: " + reslut);
							}
						}else{
							if (lastTime == null
									&& FileCacheUtil.containFile(filePath)) {
								System.out.println("load file [absolutepath]:{"
										+ filePath + "} result: yes");
								logger.info("load file [absolutepath]:{" + filePath
										+ "} result:yes ");
							} else {
								String reslut = FileCacheUtil.reLoadFile(filePath);
								System.out.println("reload file [absolutepath]:{"
										+ filePath + "} result: " + reslut);
								logger.info("reload file [absolutepath]:{" + filePath
										+ "} result: " + reslut);
							}
						}
						
						publish(tempPath, filePath);
						
					}
				}
			} else if (file.isDirectory()) {
				File[] tempFiles = file.listFiles();
				if(tempFiles != null && tempFiles.length > 0){
					for (File temp : tempFiles) {
						reloadFile(temp,relativeClassPath);
					}
				}
			}
		}
	}
	
	private void publish(final String classpath,final String localpath){
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				if(ZKCurator.checkState()){
					ZKCurator.uploadIfNotExists(classpath, localpath);
				}
			}
		});
	}

	public void init() {
		if (!IS_LOAD.getAndSet(true)) {
			long myIntervalTime = 5000;
			if (CommonUtil.isNumeric(intervalTime)) {
				myIntervalTime = Integer.valueOf(intervalTime) * 1000L;
			}
            reloadAll = fileTypeList.size() == 0;

			timer = new Timer("file-load-timer");
			timer.scheduleAtFixedRate(new TimerTask() {

				public void run() {
					File classFile = new File(CLASS_PATH);
					if (classFile.exists() && classFile.isDirectory()) {
						File[] files = classFile.listFiles();
						if (files == null || files.length == 0) {
							logger.info("classpath: {" + CLASS_PATH
									+ "} is a empty directory");
						} else {
							for (File file : files) {
								try {
									reloadFile(file,true);
								} catch (Exception e) {
									logger.error("reload file exception", e);
								}
							}
						}
					} else {
						logger.info("classpath: {" + CLASS_PATH
								+ "} is not found or is not directory");
					}
					
					if(includeList.size()>0){
						for(String str:includeList){
							if(CommonUtil.objectIsNotNull(str)){
								File file=new File(str);
								if(file.exists()){
									reloadFile(file, false);
								}
							}
						}
					}
				}
			}, 30 * 1000 + myIntervalTime, myIntervalTime);
			logger.info("PropertiesFileLoader timer init");
			System.out.println("PropertiesFileLoader timer init");
			logger.info("classpath:"+CLASS_PATH);
			System.out.println("classpath:"+CLASS_PATH);
			
			executorService.execute(new Runnable() {
				@Override
				public void run() {
					ZKCurator.checkState();
				}
			});
			
		}
	}

	public void destroy() {
		destory();
	}
	
	public void destory() {
		if (timer != null) {
			try {
				timer.cancel();
			} catch (Exception e) {
				logger.error("PropertiesFileLoader timer destroy fail", e);
			} finally {
				timer = null;
				IS_LOAD.set(false);
				logger.info("PropertiesFileLoader timer destroy");
			}
		}
		
		ZKCurator.close();
		
		executorService.shutdown();
	}
	
	public static final ClassLoader getDefaultClassLoader() {
		ClassLoader cl = null;
		try {
			cl = Thread.currentThread().getContextClassLoader();
		}
		catch (Throwable ex) {
		}
		if (cl == null) {
			cl = PropertiesFileLoader.class.getClassLoader();
			if (cl == null) {
				try {
					cl = ClassLoader.getSystemClassLoader();
				}
				catch (Throwable ex) {
				}
				if (cl == null) {
					System.out.println("load ClassLoader fail");
					logger.info("load ClassLoader fail");
				}else{
					System.out.println("load ClassLoader success: ClassLoader.getSystemClassLoader()");
					logger.info("load ClassLoader success: ClassLoader.getSystemClassLoader()");
				}
			}else{
				System.out.println("load ClassLoader success: this.class.getClassLoader()");
				logger.info("load ClassLoader success: this.class.getClassLoader()");
			}
		}else{
			System.out.println("load ClassLoader success: Thread.currentThread().getContextClassLoader()");
			logger.info("load ClassLoader success: Thread.currentThread().getContextClassLoader()");
		}
		return cl;
	}
}
