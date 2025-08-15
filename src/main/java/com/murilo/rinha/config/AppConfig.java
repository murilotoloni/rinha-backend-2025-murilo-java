package com.murilo.rinha.config;

public class AppConfig {

    public static String getProperty(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null ? defaultValue : value;
    }
    
    public static int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    public static String getMainProcessorHost() {
        return getProperty("MAIN_PROCESSOR_HOST", "localhost");
    }
    
    public static String getMainProcessorPort() {
        return getProperty("MAIN_PROCESSOR_PORT", "8001");
    }
    
    public static String getFallbackProcessorHost() {
        return getProperty("FALLBACK_PROCESSOR_HOST", "localhost");
    }
    
    public static String getFallbackProcessorPort() {
        return getProperty("FALLBACK_PROCESSOR_PORT", "8002");
    }

    public static int getDlqBufferSize() {
        return getIntProperty("RINHA_DLQ_BUFFER_SIZE", 7000);
    }
    
    public static int getDlqWorkerNum() {
        return getIntProperty("RINHA_DLQ_WORKER_NUM", 1);
    }
} 