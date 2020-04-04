package io.vantiq.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CSVConnectorConfig {

    private static final Logger LOG = LoggerFactory.getLogger(CSVConnectorConfig.class);

    private String csvLocation;
    private Integer packageSize;
    private Integer logFreq;
    private Integer sleepInterval;

    public CSVConnectorConfig(Map<String, String> sourceConfig) {
        csvLocation = sourceConfig.get("csv_location");
        packageSize = Integer.valueOf(sourceConfig.getOrDefault("package_size", "10"));
        logFreq = Integer.valueOf(sourceConfig.getOrDefault("log_freq", "100"));
        sleepInterval = Integer.valueOf(sourceConfig.getOrDefault("sleep_interval", "100"));

        if (csvLocation == null || csvLocation.length() == 0) {
            LOG.error("Invalid parameters. csvLocation must be provided");
            throw new RuntimeException("Invalid parameters in configuration file");
        }

        LOG.debug("Connector Config finish: \ncsvLocation: {}, packageSize: {}, logFreq: {}, sleepInterval:{}",
                csvLocation, packageSize, logFreq, sleepInterval);
    }

    public String getCsvLocation() {
        return csvLocation;
    }

    public Integer getPackageSize() {
        return packageSize;
    }

    public Integer getLogFreq() {
        return logFreq;
    }

    public Integer getSleepInterval() {
        return sleepInterval;
    }
}
