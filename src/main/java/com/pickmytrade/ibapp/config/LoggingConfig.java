package com.pickmytrade.ibapp.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoggingConfig {
    private static final Logger log = LoggerFactory.getLogger(LoggingConfig.class);
    private static final int MAX_LOG_FILES = 5;
    private static final long TWO_DAYS_MILLIS = 2 * 24 * 60 * 60 * 1000; // 2 days in milliseconds

    public static void cleanupOldLogs() {
        Path logDir = Config.LOG_DIR; // Use LOG_DIR from Config
        try (Stream<Path> files = Files.list(logDir)) {
            // Collect log files starting with "log.log" or matching rolled-over pattern
            List<Path> logFiles = files
                    .filter(p -> p.getFileName().toString().startsWith("log.log") || p.getFileName().toString().matches("log\\.\\d{4}-\\d{2}-\\d{2}\\.\\d+\\.log"))
                    .sorted(Comparator.comparing(p -> {
                        try {
                            return Files.getLastModifiedTime(p).toMillis();
                        } catch (IOException e) {
                            return 0L;
                        }
                    }, Comparator.reverseOrder())) // Sort by last modified time, newest first
                    .collect(Collectors.toList());

            // Delete files beyond the max limit (keep newest 5)
            if (logFiles.size() > MAX_LOG_FILES) {
                for (Path p : logFiles.subList(MAX_LOG_FILES, logFiles.size())) {
                    try {
                        Files.delete(p);
                        log.info("Deleted old log file (exceeded max count): {}", p);
                    } catch (IOException e) {
                        log.error("Error deleting log file {}: {}", p, e.getMessage());
                    }
                }
            }

            // Delete files older than 2 days
            logFiles.stream()
                    .filter(p -> {
                        try {
                            FileTime creationTime = Files.getLastModifiedTime(p);
                            return System.currentTimeMillis() - creationTime.toMillis() > TWO_DAYS_MILLIS;
                        } catch (IOException e) {
                            log.error("Error checking last modified time for {}: {}", p, e.getMessage());
                            return false;
                        }
                    })
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                            log.info("Deleted old log file (older than 2 days): {}", p);
                        } catch (IOException e) {
                            log.error("Error deleting log file {}: {}", p, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.error("Error listing log files in {}: {}", logDir, e.getMessage());
        }
    }
}