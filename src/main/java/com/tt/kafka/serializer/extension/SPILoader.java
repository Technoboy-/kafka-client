package com.tt.kafka.serializer.extension;

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
public class SPILoader<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SPILoader.class);

	private static final String SERIALIZER_DIRECTORY = "META-INF/serializer/";
	
	private static final ConcurrentMap<Class<?>, SPILoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<Class<?>, SPILoader<?>>();
	
	private final Class<?> type;
	
	private final Map<String, Object> extensionClasses;

	private SPILoader(Class<?> type) {
		this.type = type;
		extensionClasses = new HashMap<String, Object>();
		loadExtensionClasses();
	}
	
	public T getExtension() {
		return getExtension(this.type.getAnnotation(SPI.class).value());
	} 
	
	public T getExtension(String name) {
		if (name == null || name.length() == 0)
		    throw new IllegalArgumentException("Extension name == null");
		return (T) extensionClasses.get(name);
	} 
	
	public static <T> SPILoader<T> getSPIClass(Class<T> clazz){
		SPILoader<T> spiLoader = (SPILoader<T>)EXTENSION_LOADERS.get(clazz);
		if(spiLoader == null){
			EXTENSION_LOADERS.putIfAbsent(clazz, new SPILoader<T>(clazz));
			spiLoader = (SPILoader<T>)EXTENSION_LOADERS.get(clazz);
		}
		return spiLoader;
	}

	private void loadExtensionClasses() {
		try {
			loadFile(extensionClasses, SERIALIZER_DIRECTORY);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void loadFile(Map<String, Object> extensionClasses, String dir) throws Exception{
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
										extensionClasses.put(name, clazz.newInstance());
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
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
}
