package com.groupon.messagebus.client;

import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.api.Producer;
import com.groupon.messagebus.api.ProducerConfig;
import com.groupon.messagebus.api.exceptions.BrokerConnectionCloseFailedException;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.api.exceptions.SendFailedException;
import com.groupon.messagebus.api.exceptions.TooManyConnectionRetryAttemptsException;
import com.groupon.stomp.StompConnection;

/**
 * Implementation of Producer Interface Provides actual implementation of
 * connecting to the broker and provides two ways of sending message to the
 * broker.
 * 
 * Copyright (c) 2013, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
public class ProducerImpl implements Producer {

    public static final String SCHEDULED_MESSAGES_DELIVERY_TIME_MS = "scheduled_delivery_time";

    private Logger log = Logger.getLogger(ProducerImpl.class);
    private Object connectionAccessLock = new Object();

    private static final String TOPIC_NAME_PREFIX = "jms.topic.";
    private static final String QUEUE_NAME_PREFIX = "jms.queue.";
    private final int MAX_RETRY_COUNT = 3;
    private final long REST_INTERVAL = 1000;
    private final StompConnection connection;
    private ProducerConfig config;
    private Timer refreshConnectionTimer = new Timer();
    private long sendStartTime = 0;
    private Status status;

    public Status getStatus() {
        return status;
    }

    Gson gson = new Gson();

    public ProducerImpl() {
        this(new StompConnection());
    }

    public ProducerImpl(StompConnection connection) {
        this.connection = connection;
        this.status = Status.INITIALIZED;
    }

    /**
     * Starts connection with the broker.
     */
    @Override
    public void start(ProducerConfig aConfig) throws InvalidConfigException,
            TooManyConnectionRetryAttemptsException, InvalidStatusException {

        switch (this.status) {
        case INITIALIZED:
            break;
        default:
            throw new InvalidStatusException(
                    "Producer cannot be started. Status=" + this.status);
        }

        config = aConfig;

        validateConfigs(config);

        startConnection();

        refreshConnectionTimer.schedule(new RefreshConnectionTimerTask(this),
                config.getConnectionLifetime(), config.getConnectionLifetime());
        this.status = Status.RUNNING;
    }

    /**
     * Stops connection. Also takes care of closing TimerTask
     */
    @Override
    public void stop() throws BrokerConnectionCloseFailedException,
            InvalidStatusException {
        switch (this.status) {

        case RUNNING:
            break;
        case STOPPED:
            log.info("Producer is already stopped, nothing to do.");
            return;
        case INITIALIZED:
            throw new InvalidStatusException(
                    "Producer cannot be stopped. Status=" + this.status);
        }

        refreshConnectionTimer.cancel();
        stopConnection();
        this.status = Status.STOPPED;
    }

    /**
     * Sends message to the broker in fire and forget mode. Using this API is
     * fast but less reliable, compared with sendSafe() Returns false, if send
     * failes for any reason
     */
    @Override
    public void send(Message message)
            throws TooManyConnectionRetryAttemptsException, SendFailedException {
        sendInternal(message, null, false, null);
    }

    /**
     * Sends message to the broker in fire and forget mode. Using this API is
     * fast but less reliable, compared with sendSafe() Returns false, if send
     * failes for any reason
     */
    @Override
    public void send(Message message, Map<String, String> headers)
            throws TooManyConnectionRetryAttemptsException, SendFailedException {
        sendInternal(message, headers, false, null);
    }

    @Override
    public void send(Message message, String destinationName,
            Map<String, String> headers)
            throws TooManyConnectionRetryAttemptsException, SendFailedException {
        sendInternal(message, headers, false, destinationName);
    }

    /**
     * Reliable protocol of sending data to the broker returns false, if send
     * fails for any reason
     * 
     * @throws SendFailedException
     * @throws TooManyConnectionRetryAttemptsException
     * 
     */
    @Override
    public void sendSafe(Message message)
            throws TooManyConnectionRetryAttemptsException, SendFailedException {
        sendInternal(message, null, true, null);
    }

    /**
     * Reliable protocol of sending data to the broker returns false, if send
     * fails for any reason
     * 
     * @throws SendFailedException
     * @throws TooManyConnectionRetryAttemptsException
     * 
     */
    @Override
    public void sendSafe(Message message, Map<String, String> headers)
            throws TooManyConnectionRetryAttemptsException, SendFailedException {
        sendInternal(message, headers, true, null);
    }

    @Override
    public void sendSafe(Message message, String destinationName,
            Map<String, String> headers)
            throws TooManyConnectionRetryAttemptsException, SendFailedException {
        sendInternal(message, headers, true, destinationName);
    }

    /**
     * Refreshes connection. Also reports connection statistics.
     */
    public void refreshConnection()
            throws TooManyConnectionRetryAttemptsException {
        if (this.status != Status.RUNNING) {
            log.warn("This producer is not running, skip refreshing connection.");
            return;
        }
        try {
            log.debug("Refreshing connection with the broker "
                    + config.getBroker());
            synchronized (connectionAccessLock) {
                stopConnection();
                startConnection();
            }
        } catch (Exception e) {
            log.error("Failed to refresh connection with the broker for config:"
                    + gson.toJson(config));
        }

    }

    private void sendInternal(Message message, Map<String, String> headers,
            boolean isSafeSend, String destinationName)
            throws SendFailedException, TooManyConnectionRetryAttemptsException {
        if (this.status != Status.RUNNING) {
            throw new InvalidStatusException(
                    "This producer is not running and cannot publish. Status="
                            + this.status);

        }

        if (destinationName == null)
            destinationName = config.getDestinationName();
        int attempt = 0;
        boolean done = false;
        while (!done && attempt++ < config.getPublishMaxRetryAttempts()) {
            try {
                synchronized (connectionAccessLock) {

                    sendStartTime = System.currentTimeMillis();
                    if (config.isVerboseLog()) {
                        log.info("Publishing to destination_name="
                                + destinationName
                                + ", message_id="+ message.getMessageId()
                                + ", message contents=" + message.getMessageInternal().toString());
                    }
                    if (isSafeSend) {
                        connection.sendSafe(destinationName,
                                Utils.getThriftDataAsBytes(message), headers);
                    } else {
                        connection.send(destinationName,
                                Utils.getThriftDataAsBytes(message), headers);
                    }
                    if (config.isVerboseLog()) {
                        log.info("result=success destination_name=" + destinationName
                                + ", message_id=" + message.getMessageId()
                                + ", duration=" + (System.currentTimeMillis() - sendStartTime)
                                + "ms");
                    }
                    done = true;
                }
            } catch (Exception e) {
                log.warn(
                        "Failed attempt (" + attempt + "/"
                                + config.getPublishMaxRetryAttempts()
                                + ") sending message to the broker "
                                + config.getBroker()
                                + ". Refreshing connection.", e);
                refreshConnection();
            }
        }

        if (!done) {
            log.error("Failed to publish result=fail destination_name=" + destinationName
                    + ", message_id=" + message.getMessageId()
                    + ", duration=" + (System.currentTimeMillis() - sendStartTime)
                    + "ms" 
                    + ", message_contents=" + message.getMessageInternal());
            throw new SendFailedException("Failed to send message in "
                    + config.getPublishMaxRetryAttempts()
                    + " attempts to broker:" + config.getBroker());
        }
    }

    private void stopConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            log.error("Error while closing connection with the broker "
                    + config.getBroker(), e);
        }
    }

    private void startConnection()
            throws TooManyConnectionRetryAttemptsException {
        int retryCount = 0;
        boolean done = false;
        while (!done && retryCount++ < MAX_RETRY_COUNT) {
            try {
                connection.open(config.getBroker().getHost(), config
                        .getBroker().getPort());
                connection.connect(config.getUserName(), config.getPassword());
                done = true;
            } catch (Exception e) {
                log.debug(
                        "Exception connecting to the broker " + config.getBroker()
                                + " retrying attempt:" + retryCount, e);
                Utils.sleep(REST_INTERVAL);
            }
        }

        if (!done) {
            throw new TooManyConnectionRetryAttemptsException(
                    "Max connection attempts reached. Can not connect to the broker "
                            + config.getBroker());
        }
    }

    private void validateConfigs(ProducerConfig aConfig)
            throws InvalidConfigException {
        if (aConfig.getDestinationName() == null) {
            throw new InvalidConfigException("Destination name can not be null");

        }
        if (aConfig.getDestinationType() == DestinationType.QUEUE) {
            if (config.getDestinationName().indexOf(QUEUE_NAME_PREFIX) != 0) {
                String message = "Invalid destination/queue name: "
                        + config.getDestinationName()
                        + ". Queue name must start with " + QUEUE_NAME_PREFIX;
                log.error(message);
                throw new InvalidConfigException(message);
            }
        }
        if (aConfig.getDestinationType() == DestinationType.TOPIC) {
            if (config.getDestinationName().indexOf(TOPIC_NAME_PREFIX) != 0) {
                String message = "Invalid destination/topic name: "
                        + config.getDestinationName()
                        + ". Topic name must start with " + TOPIC_NAME_PREFIX;
                log.error(message);
                throw new InvalidConfigException(message);

            }
        }
    }
}

/**
 * We reset connection ever so often as configured. When scheduled, this timer
 * task refreshes the connection
 * 
 * @author ameya
 */
class RefreshConnectionTimerTask extends TimerTask {
    private Logger log = Logger.getLogger(ProducerImpl.class);
    ProducerImpl producer;

    public RefreshConnectionTimerTask(ProducerImpl aProducer) {
        producer = aProducer;
    }

    @Override
    public void run() {
        try {
            producer.refreshConnection();
        } catch (Exception e) {
            log.error("Could not refresh connection", e);
        }
    }

}
