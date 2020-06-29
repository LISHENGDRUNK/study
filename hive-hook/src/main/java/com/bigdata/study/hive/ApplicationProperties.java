package com.bigdata.study.hive;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Iterator;

/**
 * 加载配置文件
 */
public class ApplicationProperties extends PropertiesConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);
    public static final String APPLICATION_PROPERTIES = "hook.properties";
    private static volatile Configuration instance = null;

    public ApplicationProperties(URL url) throws ConfigurationException {
        super(url);
    }


    public static Configuration get() throws Exception {
        if (instance == null) {
            synchronized (ApplicationProperties.class) {
                if (instance == null) {
                    instance = get(APPLICATION_PROPERTIES);
                }
            }
        }
        return instance;
    }


    public static Configuration get(String fileName) throws Exception {

        try {
           URL url = ApplicationProperties.class.getClassLoader().getResource(fileName);
            if (url == null) {
                LOG.info("Looking for /{} in classpath", fileName);
                url = ApplicationProperties.class.getClassLoader().getResource("/" + fileName);
            }
            ApplicationProperties appProperties = new ApplicationProperties(url);
            Configuration configuration = appProperties.interpolatedConfiguration();
            logConfiguration(configuration);
            return configuration;
        } catch (Exception e) {
            throw new Exception("Failed to load application properties", e);
        }
    }


    private static void logConfiguration(Configuration configuration) {
        if (LOG.isDebugEnabled()) {
            Iterator<String> keys = configuration.getKeys();
            LOG.debug("Configuration loaded:");
            while (keys.hasNext()) {
                String key = keys.next();
                LOG.debug("{} = {}", key, configuration.getProperty(key));
            }
        }
    }


    public static Configuration getSubsetConfiguration(Configuration inConf, String propertyPrefix) {
       return inConf.subset(propertyPrefix);
    }
}