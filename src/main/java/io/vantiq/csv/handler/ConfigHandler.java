package io.vantiq.csv.handler;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.vantiq.csv.CSVConnector;
import io.vantiq.csv.CSVConnectorConfig;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(ConfigHandler.class);

    private static final String CONFIG = "config";

    private CSVConnector connector;

    public ConfigHandler(CSVConnector connector) {
        this.connector = connector;
    }

    /**
     *
     * @param message   A message to be handled
     */
    @Override
    public void handleMessage(ExtensionServiceMessage message) {
        LOG.info("Configuration for source:{}", message.getSourceName());
        Map<String, Object> configObject = (Map) message.getObject();
        Map<String, String> sourceConfig;

        // Obtain entire config from the message object
        if (!(configObject.get(CONFIG) instanceof Map)) {
            LOG.error("Configuration failed. No configuration suitable for AMQP Connector.");
            failConfig();
            return;
        }

        sourceConfig = (Map) configObject.get(CONFIG);
        CSVConnectorConfig csvConfig = new CSVConnectorConfig(sourceConfig);

        String csvLocation = csvConfig.getCsvLocation();
        File csvFile = new File(csvLocation);

        try {
            CsvParserSettings settings = new CsvParserSettings();
            settings.getFormat().setDelimiter("||");
            CsvParser parser = new CsvParser(settings);
            FileReader reader = new FileReader(csvFile);
            parser.beginParsing(reader);

            int count = 0;
            String[] row;
            while ((row = parser.parseNext()) != null) {
                if (row.length == 0) {
                    continue;
                }

                Map data = new HashMap();
                List lineData = new ArrayList(3);
                lineData.add(row[0]);
                lineData.add(row[1]);
                lineData.add(row[2]);
                data.put("data", lineData);

                count++;
                if (count % csvConfig.getLogFreq() == 0) {
                    long theTime = System.currentTimeMillis();
                    LOG.debug("line:{}\t{}", theTime, data);
                    if (csvConfig.getSleepInterval() > 0) {
//                        Thread.sleep(csvConfig.getSleepInterval());
                    }
                }
            }
            LOG.info("line processed:{}", count);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * Closes the source {@link CSVConnector} and marks the configuration as completed. The source will
     * be reactivated when the source reconnects, due either to a Reconnect message (likely created by an update to the
     * configuration document) or to the WebSocket connection crashing momentarily.
     */
    private void failConfig() {
//        connector.close();
    }

}
