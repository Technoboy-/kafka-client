package com.owl.kafka.client.spi;

import com.owl.kafka.client.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @Author: Tboy
 */
@SuppressWarnings("unchecked")
public abstract class SPILoader<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SPILoader.class);

	private static final String SPI_DIRECTORY = "META-INF/spiconfig/";

	protected static final ConcurrentMap<Class<?>, SPILoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<Class<?>, SPILoader<?>>();

	protected final Map<String, Object> extensionClasses = new HashMap<>();
	
	public T getExtension(String name) {
		if (StringUtils.isBlank(name))
		    throw new IllegalArgumentException("Extension name is null");
		return (T) extensionClasses.get(name);
	}

	protected <T> void loadExtensionClasses(Class<T> clazz) {
		try {
			loadFile(clazz, extensionClasses, SPI_DIRECTORY);
		} catch (Exception e) {
			LOGGER.error("loadExtensionClasses error", e);
		}
	}

	protected abstract T newInstance(Class<?> type);

	private <T> void loadFile(Class<T> type, Map<String, Object> extensionClasses, String dir) throws Exception{
		String fileName = dir + type.getName();
		Enumeration<java.net.URL> urls;
		ClassLoader classLoader = SPILoader.class.getClassLoader();
		if (classLoader != null) {
			urls = classLoader.getResources(fileName);
		} else {
			urls = ClassLoader.getSystemResources(fileName);
		}
		if (urls != null) {
			while (urls.hasMoreElements()) {
				java.net.URL url = urls.nextElement();
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new InputStreamReader(url.openStream(), "utf-8"));
					String line = null;
					while ((line = reader.readLine()) != null) {
						final int ci = line.indexOf('#');
						if (ci >= 0)
							line = line.substring(0, ci);
						line = line.trim();
						if (line.length() > 0) {
							try {
								String name = null;
								int i = line.indexOf('=');
								if (i > 0) {
									name = line.substring(0, i).trim();
									line = line.substring(i + 1).trim();
								}
								if (line.length() > 0) {
									Class<?> clazz = Class.forName(line, true, classLoader);
									if (!type.isAssignableFrom(clazz)) {
										throw new IllegalStateException("Error when load extension class(interface: " + type + ", class line: " + clazz.getName() + "), class " + clazz.getName() + "is not subtype of interface.");
									} else {
										extensionClasses.put(name, newInstance(clazz));
									}
								}
							} catch (Throwable t) {
								IllegalStateException e = new IllegalStateException("Failed to load extension class(interface: " + type + ", class line: " + line + ") in " + url + ", cause: " + t.getMessage(), t);
								throw e;
							} 
						}
					}
				} catch (Throwable t) {
					LOGGER.error("Exception when load extension class(interface: " + type + ", description file: " + fileName + ").", t);
				} finally {
					if(reader != null){
						try {
							reader.close();
						} catch (IOException e) {
							LOGGER.error("close reader error", e);
						}
					}
				}
			}
		}
	}
}
