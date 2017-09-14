package com.efun.mainland.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.util.CollectionUtils;

/**
 * 属性文件加载器<br/>
 * <font color=red> need to set the system property efunSystemPropertyRegion to
 * value <br/>
 * eg:efunSystemPropertyRegion=tw <br/>
 * default efunSystemPropertyRegion is null </font>
 * 
 * @author Efun
 *
 */
public class PropertyConfigurer extends PropertyPlaceholderConfigurer {

	private static final Logger logger = LoggerFactory.getLogger(PropertyConfigurer.class);

	private static final String CLASSPATH = "classpath:";
	private static final String FILEPATH = "file:";
	private static final String SYSTEM_PROPERTY_REGION_KEY = "efunSystemPropertyRegion";
	private static String SYSTEM_PROPERTY_REGION_VALUE = null;

	private List<Properties> mpropList = new ArrayList<Properties>();
	protected String prop;
	protected String[] props;
	protected List<String> propList;


	public void setProp(String prop) {
		this.prop = prop;

		load(prop);
	}

	public void setProps(String[] props) {
		this.props = props;

		if (props != null && props.length > 0) {
			for (String prop : props) {
				load(prop);
			}
		}
	}

	public void setPropList(List<String> propList) {
		this.propList = propList;

		if (propList != null && propList.size() > 0) {
			for (String prop : propList) {
				load(prop);
			}
		}
	}

	public static final String getRegionSystemProperty() {
		if (SYSTEM_PROPERTY_REGION_VALUE == null) {
			SYSTEM_PROPERTY_REGION_VALUE = System.getProperty(SYSTEM_PROPERTY_REGION_KEY);
		}
		return SYSTEM_PROPERTY_REGION_VALUE;
	}

	@Override
	protected Properties mergeProperties() throws IOException {
		// TODO Auto-generated method stub
		Properties result = super.mergeProperties();
		if (this.mpropList.size() > 0) {
			for (Properties localProp : this.mpropList) {
				CollectionUtils.mergePropertiesIntoMap(localProp, result);
			}
			if (!this.localOverride) {
				// Load properties from file afterwards, to let those properties
				// override.
				loadProperties(result);
			}
		}

		return result;
	}

	/**
	 * 加载预加载文件，例如数据源。
	 * 如果系统设置的地区环境变量，默认规则下文件目录下回自动增加地区子文件夹。
	 * @param path
     */
	protected void load(String path) {
		if (StringUtils.isNotBlank(path)) {
			//classpath方式读取
			if (path.startsWith(CLASSPATH)) {
				String classpath = PropertiesFileLoader.getClassPath();
				String area = getRegionSystemProperty();
				if (area != null) {
					classpath += area;
					classpath += File.separatorChar;
				}
				path = classpath + path.substring(CLASSPATH.length());

				logger.info("resources path:" + classpath);
				System.out.println("resources path:" + classpath);

			//文件系统方式读取
			} else if (path.startsWith(FILEPATH)) {
				String area = getRegionSystemProperty();
				if (area != null) {
					path = path.substring(0, path.lastIndexOf(File.separatorChar) + 1) + area + path.substring(path.lastIndexOf(File.separatorChar));
				}
				path = path.replace(FILEPATH, "");
				logger.info("resources path:" + path);
				System.out.println("resources path:" + path);
			}
			File file = new File(path);
			if (file.exists()) {
				if (file.isFile()) {
					System.out.println("path is file:" + path);
					logger.info("path is file:" + path);
					FileInputStream fis = null;
					try {
						fis = new FileInputStream(file);
						Properties p = new Properties();
						p.load(fis);
						mpropList.add(p);
					} catch (Exception e) {
						if (fis != null) {
							try {
								fis.close();
							} catch (IOException e1) {
							}
						}
					}
				} else {
					System.out.println("path is not file:" + path);
					logger.info("path is not file:" + path);
				}
			} else {
				System.out.println("path not exists:" + path);
				logger.info("path not exists:" + path);
			}
		}
	}

}
