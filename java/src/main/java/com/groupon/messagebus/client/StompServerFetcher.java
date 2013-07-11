package com.groupon.messagebus.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;

import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.ConsumerConfig;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.exceptions.AckFailedException;
import com.groupon.messagebus.api.exceptions.BrokerConnectionFailedException;
import com.groupon.messagebus.api.exceptions.InvalidDestinationException;
import com.groupon.messagebus.api.exceptions.KeepAliveFailedException;
import com.groupon.messagebus.api.exceptions.NackFailedException;
import com.groupon.messagebus.api.exceptions.TooManyConnectionRetryAttemptsException;
import com.groupon.stomp.Stomp;
import com.groupon.stomp.Stomp.Headers.Subscribe;
import com.groupon.stomp.StompConnection;
import com.groupon.stomp.StompFrame;

/**
 * Start a thread. This prefetchs the value from the broker and keeps it in its
 * internal cache
 */
public class StompServerFetcher implements Runnable {

    private Logger log = Logger.getLogger(StompServerFetcher.class);
    private String host;
    private static long FETCHER_TIMEOUT = 300000;
    private static long FAILURE_RETRY_INTERVAL = 60000;
    private Map<String, Object> ackSafeLockMap = new Hashtable<String, Object>();
    private Map<String, StompFrame> receiptFrameMap = new Hashtable<String, StompFrame>();

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    private int port;
    private ConsumerConfig config = null;
    private long receiveStartTime = 0;
    private long connStartTime = 0;
    private volatile boolean keepRunning = false;
    private StompConnection connection = null;

    public StompConnection getConnection() {
        return connection;
    }

    public void setConnection(StompConnection connection) {
        this.connection = connection;
    }

    private Object connectionAccessLock = new Object();
    private final long REST_INTERVAL = 1000;
    private final int MAX_RETRY_COUNT = 3;

    private LinkedBlockingQueue<StompFrame> preFetchedCache = new LinkedBlockingQueue<StompFrame>();
    private volatile StompFrame lastSentMessage;

    /**
     * Creates StompServerFetcher. You need to specify which specific host/port
     * to connect to, and the ConsumerConfig object for rest of the
     * configurations
     * 
     * @param aHost
     * @param aPort
     * @param aConfig
     */
    public StompServerFetcher(String aHost, int aPort, ConsumerConfig aConfig) {
        this(aHost, aPort, aConfig, new StompConnection());
    }

    public StompServerFetcher(String aHost, int aPort, ConsumerConfig aConfig,
            StompConnection aConnection) {
        host = aHost;
        port = aPort;
        config = aConfig;
        connection = aConnection;
        keepRunning = true;
    }

    /**
     * Start a thread. This pre fetches the value from the broker and keeps it
     * in its internal cache
     */
    @Override
    public void run() {
        while (keepRunning) {
            try {
                refreshConnection();
                preFetchMessage();
            } catch (TooManyConnectionRetryAttemptsException tme) {
                log.info(
                        tme.getMessage() + " going to sleep for "
                                + FAILURE_RETRY_INTERVAL + " ms", tme);
                try {
                    Thread.sleep(FAILURE_RETRY_INTERVAL);
                } catch (InterruptedException e) {
                    log.info("Interrupted: ", e);
                }
            } catch (BrokerConnectionFailedException be) {
                connStartTime = 0;
                log.debug("Error connecting to the broker at " + host + ":"
                        + port, be);
            } catch (Exception e) {
                log.info("Exception in thread:", e);
            }
        }
    }

    /**
     * Non blocking receive. Polls from its internal cache and returns. In case
     * its non null value, keeps pointer to the frame for ack to use the frame.
     * 
     * @return
     */
    public StompFrame receiveLast() {
        StompFrame result = preFetchedCache.poll();
        if (result != null) {
            lastSentMessage = result;
            try {
                synchronized (this.connectionAccessLock) {
                    try {
                        connection.credit(result);
                    } catch (IOException ie) {
                        log.warn(
                                "IOException received while sending credit. Retrying connection.",
                                ie);
                        retryConnection();
                        connection.credit(result);
                    }
                }
            } catch (Exception e) {
                log.error("Exception while sending credit message.", e);
            }
            if (config.getAckType() == ConsumerAckType.AUTO_CLIENT_ACK) {
                try {
                    ack();
                } catch (AckFailedException e) {
                    log.error(
                            "Failed to auto-ack message:\n" + result.getBody()
                                    + "\nExpect to receive this message again",
                            e);
                    return null;
                }
            }
        }

        return result;
    }

    /**
     * In client_ack mode, client is expected to ack. This method acks back to
     * server Also, unblocks thread to pull in next message from the broker.
     * 
     * @throws AckFailedException
     * 
     * @throws Exception
     */
    public void ack() throws AckFailedException {

        if (null != lastSentMessage)
            ack(lastSentMessage.getHeaders().get(
                    Stomp.Headers.Message.MESSAGE_ID), lastSentMessage
                    .getHeaders().get("connection-id"));
        else {
            log.warn("WARNING: Unidentified ack message. This may happen in case client sends ack before receiving the message, ignoring ...");
        }
    }

    public void ack(String messageId, String connectionId)
            throws AckFailedException {
        try {
            synchronized (connectionAccessLock) {
                try {
                    connection.ack(messageId, null, config.getSubscriptionId(),
                            connectionId, null);
                } catch (IOException ie) {
                    log.warn("IOException received while sending ack. Retrying connection.",ie);
                    retryConnection();
                    connection.ack(messageId, null, config.getSubscriptionId(),
                            connectionId, null);
                }
            }
        } catch (Exception e) {
            throw new AckFailedException(e);
        }
    }

    public void ackSafe(long timeout) throws AckFailedException,
            InterruptedException {
       
        if (null != lastSentMessage) {
            ackSafe(lastSentMessage.getHeaders().get(Stomp.Headers.Message.MESSAGE_ID), null, timeout);
        }
          else {
            log.warn("WARNING: Unidentiefied ack message. This may happen in case client sends ack before receiving the message, ignoring ...");
        }
    }

    public void ackSafe(String messageId, String connectionId,
            long timeout) throws AckFailedException {
        String receipt_id = java.util.UUID.nameUUIDFromBytes(
                messageId.getBytes()).toString();
        try {
           
            Object ackLock = new Object();

            ackSafeLockMap.put(receipt_id, ackLock);

            synchronized (ackLock) {
                synchronized (connectionAccessLock) {
                    try {
                        connection.ack(messageId, null, config.getSubscriptionId(),
                                connectionId, receipt_id);
                    } catch (IOException ie) {
                        log.warn(
                                "IOException received while sending ack. Retrying connection.",
                                ie);
                        retryConnection();
                        connection.ack(messageId, null, config.getSubscriptionId(),
                                connectionId, receipt_id);
                    }
                }
                ackLock.wait(timeout);
            }
            StompFrame receivedFrame = receiptFrameMap.get(receipt_id);
            if (null == receivedFrame)
                throw new AckFailedException(
                        "Ack timed out. Failed to receive RECEIPT from server in "
                                + timeout + "ms.");
            else if (null != receivedFrame
                    && !receivedFrame.getAction().equals(
                            Stomp.Responses.RECEIPT)) {
                throw new AckFailedException("Failed to receive RECEIPT: "
                        + receivedFrame.getBody());
            }
            receiptFrameMap.remove(receipt_id);
            ackSafeLockMap.remove(receipt_id);
        } catch (Exception e) {
            throw new AckFailedException(e);
        }
    }

    public void keepAlive() throws KeepAliveFailedException {
        try {
            synchronized (connectionAccessLock) {
                try {
                    connection.keepAlive();
                } catch (IOException ie) {
                    log.warn(
                            "IOException received while sending keepalive. Retrying connection.",
                            ie);
                    retryConnection();
                    connection.keepAlive();
                }
            }
        } catch (Exception e) {
            throw new KeepAliveFailedException(e);
        }

    }

    public void nack() throws NackFailedException {
        if (null != lastSentMessage) {
           nack(lastSentMessage.getHeaders().get(Stomp.Headers.Message.MESSAGE_ID));
        } else {
            log.warn("WARNING: Unidentiefied nack message. This may happen in case client sends nack before receiving the message, ignoring ...");
        }
    }

    public void nack(String messageId) throws NackFailedException {
        try {
            synchronized (connectionAccessLock) {
                try {
                    connection.nack(messageId, config.getSubscriptionId());
                } catch (IOException ie) {
                    log.warn(
                            "IOException received while sending nack. Retrying connection.",
                            ie);
                    retryConnection();
                    connection.nack(messageId, config.getSubscriptionId());
                }
            }
        } catch (Exception e) {
            throw new NackFailedException(e);
        }
    }

    /**
     * Close the connection with the broker, and asynchronously close the thread
     * 
     * @return true/false,
     */
    public boolean close() {
        try {
            keepRunning = false;
            synchronized (connectionAccessLock) {
                this.preFetchedCache.clear();
                if (connection.isConnected()) {
                    connection.disconnect();
                    connection.close();
                }
            }
            
            log.debug("Connection with the broker " + host + ":" + port
                    + " closed successfully");
            return true;
        } catch (Exception e) {
            log.error("Error while closing the connection", e);
            return false;
        }
    }

    private void retryConnection() throws IOException,
            TooManyConnectionRetryAttemptsException {
        synchronized (connectionAccessLock) {
            connection.close();
            connect(MAX_RETRY_COUNT); // retries if fails
        }

    }

    private void preFetchMessage() throws BrokerConnectionFailedException {

        try {
            receiveStartTime = System.currentTimeMillis();
            StompFrame tmpFrame = null;

            // There may be a race condition with the refresh thread, which
            // nulls connection's StompSocket occationally.
            // We can't guard the below line to connectionAccessLock since it
            // may wait for server communication.
            try {
                tmpFrame = connection.receive(FETCHER_TIMEOUT);
                if (null != tmpFrame
                        && tmpFrame.getHeaders().get("receipt-id") != null) {
                    String receipt_id = tmpFrame.getHeaders().get("receipt-id");
                    Object ackLock = ackSafeLockMap.get(receipt_id);
                    receiptFrameMap.put(receipt_id, tmpFrame);
                    if (ackLock != null) {
                        synchronized (ackLock) {
                            ackLock.notifyAll();
                        }
                        return;
                    }
                }
            } catch (IOException ee) {
                if (keepRunning) {
                    log.debug("IOException received, server may have dropped the connection. Refreshing");
                    retryConnection();
                }
                return;
            } catch (NullPointerException e) {
                log.debug("NullPointerException thrown in preFetchMessage. Possible race condition on StompConnection"
                        + e);
            }

            // When an error is received, log it and continue. It could be an
            // uncritical exception
            // like double-acking a message.
            if (null != tmpFrame && null != tmpFrame.getAction()) {
                if (tmpFrame.getAction().equals("ERROR")) {
                    log.warn("Error frame received in prefetch() from server:"
                            + this.host + "\n" + tmpFrame.getBody());
                    return;
                }

                else if (tmpFrame.getAction().equalsIgnoreCase(
                        Stomp.Responses.MESSAGE)) {
                    log.debug("Server: " + host + ":" + port
                            + " , pre-fetch request took "
                            + (System.currentTimeMillis() - receiveStartTime)
                            + " ms");
                    preFetchedCache.put(tmpFrame);
                }
            }
        } catch (Exception e) {
            if (keepRunning) {
                connStartTime = 0; // reset connection
                throw new BrokerConnectionFailedException(e);
            }
        }
    }

    private boolean isStaleConnection() throws InterruptedException {

        synchronized (this.connectionAccessLock) {

            return (!connection.isConnected());
        }
    }

    public void refreshConnection() throws BrokerConnectionFailedException {
        try {
            if (keepRunning && isStaleConnection()) {

                log.debug("Refreshing the connection with broker " + host + ":"
                        + port + " ...");
                retryConnection();
            }
        } catch (InterruptedException e) {
            throw new BrokerConnectionFailedException(e.getMessage());
        } catch (IOException e) {
            throw new BrokerConnectionFailedException(e.getMessage());
        }
    }

    private void connect(int retryAttemptLeft)
            throws TooManyConnectionRetryAttemptsException {

        synchronized (connectionAccessLock) {
            while (retryAttemptLeft != 0) {
                try {
                    retryAttemptLeft--;
                    HashMap<String, String> headers = new HashMap<String, String>();

                    // Add information about durable subscription for topics.
                    if (config.getDestinationType() == DestinationType.TOPIC) {
                        headers.put("durable-subscriber-name",
                                config.getSubscriptionId());
                        headers.put("id", config.getSubscriptionId());
                        headers.put("client-id", config.getSubscriptionId());
                    }

                    connection.open(host, port);
                    connection.connect(config.getUserName(),
                            config.getPassword(), config.getSubscriptionId());
                    connection.subscribe(config.getDestinationName(),
                            Subscribe.AckModeValues.CLIENT, headers);

                    connStartTime = System.currentTimeMillis();
                    log.debug("Connection established successfully with the broker "
                            + host + ":" + port);
                    break; // on successful connection
                } catch (Exception e) {
                    log.debug("Error connecting broker " + host + ":" + port
                            + " Retyring attempt no " + (retryAttemptLeft + 1),
                            e);
                    Utils.sleep(REST_INTERVAL);

                    if (0 == retryAttemptLeft) {
                        throw new TooManyConnectionRetryAttemptsException(
                                "Can not connect to the broker after "
                                        + MAX_RETRY_COUNT + "retry attempts");
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
