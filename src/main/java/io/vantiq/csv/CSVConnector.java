package io.vantiq.csv;

import io.vantiq.csv.handler.*;
import io.vantiq.extjsdk.ExtensionWebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.vantiq.csv.ConnectorConstants.CONNECTOR_CONNECT_TIMEOUT;
import static io.vantiq.csv.ConnectorConstants.RECONNECT_INTERVAL;


public class CSVConnector {

    static final Logger LOG = LoggerFactory.getLogger(CSVConnector.class);

    ConnectorConfig connectionInfo;
    ExtensionWebSocketClient vantiqClient = null;
    VantiqUtil vantiqUtil = new VantiqUtil();

    public CSVConnector() {
        connectionInfo = new ConnectorConfig();

        if (connectionInfo == null) {
            throw new RuntimeException("No VANTIQ connection information provided");
        }
        if (connectionInfo.getSourceName() == null) {
            throw new RuntimeException("No source name provided");
        }
    }


    public void start() {
        vantiqClient = new ExtensionWebSocketClient(connectionInfo.getSourceName());

        vantiqClient.setConfigHandler(new ConfigHandler(this));
        vantiqClient.setReconnectHandler(new ReconnectHandler(this));
        vantiqClient.setCloseHandler(new CloseHandler(this));
        vantiqClient.setPublishHandler(new PublishHandler(this));
        vantiqClient.setQueryHandler(new QueryHandler(this));

        boolean sourcesSucceeded = false;
        while (!sourcesSucceeded) {
            vantiqClient.initiateFullConnection(connectionInfo.getVantiqUrl(), connectionInfo.getToken());

            sourcesSucceeded = checkConnectionFails(vantiqClient, CONNECTOR_CONNECT_TIMEOUT);
            if (!sourcesSucceeded) {
                try {
                    Thread.sleep(RECONNECT_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.error("An error occurred when trying to sleep the current thread. Error Message: ", e);
                }
            }
        }
    }

    public ExtensionWebSocketClient getVantiqClient() {
        return vantiqClient;
    }

    public VantiqUtil getVantiqUtil() {
        return this.vantiqUtil;
    }

    public ConnectorConfig getConnectionInfo() {
        return connectionInfo;
    }

    /**
     * Waits for the connection to succeed or fail, logs and exits if the connection does not succeed within
     * {@code timeout} seconds.
     *
     * @param client    The client to watch for success or failure.
     * @param timeout   The maximum number of seconds to wait before assuming failure and stopping
     * @return          true if the connection succeeded, false if it failed to connect within {@code timeout} seconds.
     */
    public boolean checkConnectionFails(ExtensionWebSocketClient client, int timeout) {
        boolean sourcesSucceeded = false;
        try {
            sourcesSucceeded = client.getSourceConnectionFuture().get(timeout, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            LOG.error("Timeout: full connection did not succeed within {} seconds: {}", timeout, e);
        }
        catch (Exception e) {
            LOG.error("Exception occurred while waiting for webSocket connection", e);
        }
        if (!sourcesSucceeded) {
            LOG.error("Failed to connect to all sources.");
            if (!client.isOpen()) {
                LOG.error("Failed to connect to server url '" + connectionInfo.getVantiqUrl() + "'.");
            } else if (!client.isAuthed()) {
                LOG.error("Failed to authenticate within " + timeout + " seconds using the given authentication data.");
            } else {
                LOG.error("Failed to connect within 10 seconds");
            }
            return false;
        }
        return true;
    }
}
