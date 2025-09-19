package com.pickmytrade.ibapp.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Config {
    public static final String DB_URL = "jdbc:sqlite:IB.db";
    private static final String PLATFORM = System.getProperty("os.name").toLowerCase();
    private static final Path LOG_DIR_WINDOWS = Paths.get(System.getenv("APPDATA"), "PickMYTrade");
    private static final Path LOG_DIR_LINUX = Paths.get("/var/log/PickMYTrade");
    public static final Path LOG_DIR;

    static {
        if (PLATFORM.contains("linux")) {
            if (!LOG_DIR_LINUX.toFile().exists()) {
                try {
                    LOG_DIR_LINUX.toFile().mkdirs();
                    System.out.println("Created log directory: " + LOG_DIR_LINUX);
                } catch (Exception e) {
                    System.err.println("Failed to create log directory: " + e.getMessage());
                }
            }
            LOG_DIR = LOG_DIR_LINUX;
        } else {
            if (!LOG_DIR_WINDOWS.toFile().exists()) {
                try {
                    LOG_DIR_WINDOWS.toFile().mkdirs();
                    System.out.println("Created log directory: " + LOG_DIR_WINDOWS);
                } catch (Exception e) {
                    System.err.println("Failed to create log directory: " + e.getMessage());
                }
            }
            LOG_DIR = LOG_DIR_WINDOWS;
        }
        // Set log.path system property for Logback
        System.setProperty("log.path", LOG_DIR.toString().replace("\\", "/"));
    }

    public static final String LOG_TYPE = System.getenv("LOG_TYPE") != null ? System.getenv("LOG_TYPE") : "DEBUG";
    public static final Logger log = LoggerFactory.getLogger("com.pickmytrade.ibapp");
}