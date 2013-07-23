package com.groupon.messagebus.client;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;
import com.groupon.messagebus.api.Consumer;
import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.ConsumerConfig;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.exceptions.AckFailedException;
import com.groupon.messagebus.api.exceptions.BrokerConnectionFailedException;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidDestinationException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.api.exceptions.KeepAliveFailedException;
import com.groupon.messagebus.api.exceptions.NackFailedException;
import com.groupon.messagebus.api.exceptions.ReceiveTimeoutException;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.util.DynamicServerListGetter;
import com.groupon.stomp.StompFrame;

/**
 * Actual implementation of Consumer This class provides following: Starts new
 * threads with all the configured brokers. Every time receive() is called, this
 * class round robins between these threads to access next message. In a way its
 * a load balancer for these threads.
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
public class ConsumerImpl implements Consumer {

    private Logger log = Logger.getLogger(ConsumerImpl.class);
    private Timer refreshServerListTimer = new Timer();

    private List<StompServerFetcher> serverList = Collections
            .synchronizedList(new ArrayList<StompServerFetcher>());
    private Map<HostParams, List<StompServerFetcher>> currentServers = new HashMap<HostParams, List<StompServerFetcher>>();
    private StompServerFetcher lastSentServer = null;
    private int lastContactedServerIdx = -1;

    private ConsumerConfig config;
    private static final String TOPIC_NAME_PREFIX = "jms.topic.";
    private static final String QUEUE_NAME_PREFIX = "jms.queue.";
    private static final long ACKSAFE_TIMEOUT = 1000;
    private Status status;

    public Status getStatus() {
        return status;
    }

    ExecutorService executor;

    public ConsumerImpl() {
        this.status = Status.INITIALIZED;
    }

    @Override
    public boolean start(ConsumerConfig aConfig) throws InvalidConfigException,
            InvalidStatusException {

        log.debug("consumer " + this.toString() + " starting.");

        switch (this.status) {
        case INITIALIZED:
            break;
        default:
            throw new InvalidStatusException(
                    "Consumer cannot be started. Status=" + this.status);
        }

        config = aConfig;

        validateConfigs(config);

        Set<HostParams> hostsList = null;
                
        //If useDynamicServerList and url is null, generate it.
        if(config.useDynamicServerList() && null == config.getDynamicServerListFetchURL() && 
           null != config.getHostParams() && config.getHostParams().size() > 0){
            HostParams param = config.getHostParams().iterator().next();

            try {
                config.setDynamicServerListFetchURL(DynamicServerListGetter.buildDynamicServersURL(param.getHost(), 8081));
            } catch (URISyntaxException e) {
                log.error("Error creating dynamic server url from " + param, e);
            }
        }
        
        if (null != config.getDynamicServerListFetchURL()) {

            try {
                hostsList = fetchHostList();
            } catch (MalformedURLException e) {
                throw new InvalidConfigException(
                        "Invalid dynamic server list fetcher url "
                                + config.getDynamicServerListFetchURL());
            } catch (IOException e) {
                log.error(
                        "IOException when getting broker list. Aborting consumer start.",
                        e);
                
                return false;
            } catch (Exception e) {
                log.error(
                        "Something was wrong when getting broker list. Aborting consumer start.",
                        e);
                return false;
            }

            refreshServerListTimer.schedule(
                    new RefreshServerListTimerTask(this),
                    config.getConnectionLifetime(),
                    config.getConnectionLifetime());

        } else {
            hostsList = config.getHostParams();
        }

        executor = Executors
                .newFixedThreadPool(hostsList.size() > 0 ? hostsList.size() : 1);

        for (HostParams host : hostsList) {
            try {
                startAndRegisterConnection(host);
            
            } catch (BrokerConnectionFailedException e) {
                log.error("Not able to connect to broker " + host.toString()
                        + e.getStackTrace());
            }
        }

        this.status = Status.RUNNING;
        return true;
    }

    @Override
    public void stop() throws InvalidStatusException {
        switch (this.status) {

        case RUNNING:
            break;
        case STOPPED:
            log.info("Consumer is already stopped, nothing to do.");
            return;
        default:
            throw new InvalidStatusException(
                    "Consumer cannot be stopped. Status=" + this.status);
        }
        for (StompServerFetcher client : serverList) {
            client.close();
        }
        if (executor != null)
            executor.shutdownNow();
        serverList.clear();
        currentServers.clear();
        refreshServerListTimer.cancel();
        log.debug("Consumer " + this.toString() + " stopped successfully");
        this.status = Status.STOPPED;
    }

    @Override
    public boolean ack() {
        if (config.getAckType() == ConsumerAckType.AUTO_CLIENT_ACK) {
            log.warn("This consumer has auto ack type. No need to explicitly ack.");
            return false;
        }

        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not ack.");
            return false;
        }

        if (lastSentServer != null) {
            try {
                lastSentServer.ack();
                lastSentServer = null;
                return true;
            } catch (AckFailedException e) {
                log.error("Ack failed to server " + lastSentServer, e);
                return false;
            }
        }
        return false;
    }

    public boolean ack(String ackId) {
        if (config.getAckType() == ConsumerAckType.AUTO_CLIENT_ACK) {
            log.warn("This consumer has auto ack type. No need to explicitly ack.");
            return false;
        }

        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not ack.");
            return false;
        }

        try {
            String clearText = Utils.decode(ackId);

            StringTokenizer st = new StringTokenizer(clearText, ":");
            if (st.countTokens() != 4)
                throw new AckFailedException(new Exception(
                        "Wrong ack id format."));
            String hostname = st.nextToken();
            int port = Integer.parseInt(st.nextToken());
            String messageId = st.nextToken();
            String connectionId = st.nextToken();

            HostParams hostInfo = new HostParams(hostname, port);
            List<StompServerFetcher> connections = currentServers.get(hostInfo);
            if (connections == null || connections.size() == 0) {
                connections = new ArrayList<StompServerFetcher>();
                StompServerFetcher server = new StompServerFetcher(hostname,
                        port, config);
                connections.add(server);
                currentServers.put(hostInfo, connections);
                serverList.add(server);
            }

            // We should send ack from one connection only.
            try {
                connections.get(0).ack(messageId, connectionId);
                // nullifies lastSentServer so default ack() won't double-ack
                // this message
                lastSentServer = null;
                return true;
            } catch (AckFailedException e) {
                log.error("Ack failed to messageId " + messageId, e);
            }
        } catch (AckFailedException e) {
            log.error("Ack failed to ackId " + ackId, e);
            return false;
        }
        return false;
    }

    @Override
    public Message receiveImmediate() {
        return receiveImpl(false);

    }

    @Override
    public Message receive() {
        return receiveImpl(true);
    }

    @Override
    public Message receive(long timeout) throws ReceiveTimeoutException {
        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not receive. Status="
                    + this.status);
            return null;
        }

        Callable<Message> task = new Callable<Message>() {
            public Message call() {
                return receiveImpl(true);
            }
        };

        Future<Message> future = executor.submit(task);
        try {

            return future.get(timeout, TimeUnit.MILLISECONDS);

        } catch (TimeoutException tx) {
            log.debug("Receive request timed out", tx);
            throw new ReceiveTimeoutException(tx);
        } catch (InterruptedException ix) {
            log.error(ix.getMessage(), ix);
            throw new ReceiveTimeoutException(ix);
        } catch (ExecutionException ex) {
            log.error(ex.getMessage(), ex);
            throw new ReceiveTimeoutException(ex);
        } finally {
            future.cancel(true);
        }

    }

    @Override
    public boolean nack() {
        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not nack.");
            return false;
        }
        if (lastSentServer != null) {
            try {
                lastSentServer.nack();
                lastSentServer = null;
                return true;
            } catch (NackFailedException e) {
                log.error("Nack failed to server " + lastSentServer, e);
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean nack(String ackId) {
        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not nack.");
            return false;
        }
        try {
            String clearText = Utils.decode(ackId);

            StringTokenizer st = new StringTokenizer(clearText, ":");
            if (st.countTokens() != 4)
                throw new NackFailedException(new Exception(
                        "Wrong nack id format."));
            String hostname = st.nextToken();
            int port = Integer.parseInt(st.nextToken());
            String messageId = st.nextToken();

            HostParams hostInfo = new HostParams(hostname, port);
            List<StompServerFetcher> connections = currentServers.get(hostInfo);
            if (connections == null || connections.size() == 0) {
                connections = new ArrayList<StompServerFetcher>();
                StompServerFetcher server = new StompServerFetcher(hostname,
                        port, config);
                connections.add(server);
                currentServers.put(hostInfo, connections);
                serverList.add(server);
            }

            // We should send nack from one connection only.
            try {
                connections.get(0).nack(messageId);
                // nullifies lastSentServer so default nack() won't double-ack
                // this message
                lastSentServer = null;
                return true;
            } catch (NackFailedException e) {
                log.error("Nack failed to messageId " + messageId, e);
            }
        } catch (NackFailedException e) {
            log.error("nack failed to nackId " + ackId, e);
            return false;
        }
        return false;
    }

    @Override
    public boolean keepAlive() {
        boolean keepAlive = true;
        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not send keep-alive.");
            return false;
        }
        try {
            Iterator<Entry<HostParams, List<StompServerFetcher>>> hostIterator = currentServers
                    .entrySet().iterator();
            while (hostIterator.hasNext()) {
                Map.Entry<HostParams, List<StompServerFetcher>> pairs = hostIterator
                        .next();
                List<StompServerFetcher> connections = currentServers.get(pairs
                        .getKey());

                // Send KeepAlive
                try {
                    connections.get(0).keepAlive();
                } catch (KeepAliveFailedException e) {
                    log.error(
                            "Keepalive failed for broker "
                                    + connections.get(0).getHost(), e);
                    keepAlive = false;
                }
            }

        } catch (Exception e) {
            log.warn("KeepAlive failed", e);
            keepAlive = false;
        }
        // Send heartbeat to all brokers
        return keepAlive;
    }

    @Override
    public boolean ackSafe(String ackId) {
        return ackSafe(ackId, ACKSAFE_TIMEOUT);
    }

    @Override
    public boolean ackSafe(String ackId, long timeout) {
        if (config.getAckType() == ConsumerAckType.AUTO_CLIENT_ACK) {
            log.warn("This consumer has auto ack type. No need to explicitly ack.");
            return false;
        }

        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not ack.");
            return false;
        }

        try {
            String clearText = Utils.decode(ackId);

            StringTokenizer st = new StringTokenizer(clearText, ":");
            if (st.countTokens() != 4)
                throw new AckFailedException(new Exception(
                        "Wrong ack id format."));
            String hostname = st.nextToken();
            int port = Integer.parseInt(st.nextToken());
            String messageId = st.nextToken();
            String connectionId = st.nextToken();

            HostParams hostInfo = new HostParams(hostname, port);
            List<StompServerFetcher> connections = currentServers.get(hostInfo);
            if (connections == null || connections.size() == 0) {
                connections = new ArrayList<StompServerFetcher>();
                StompServerFetcher server = new StompServerFetcher(hostname,
                        port, config);
                connections.add(server);
                currentServers.put(hostInfo, connections);
                serverList.add(server);
            }

            // We should send ack from one connection only.
            try {
                connections.get(0).ackSafe(messageId, connectionId, timeout);
                // nullifies lastSentServer so default ackSafe() won't
                // double-ack this message
                lastSentServer = null;
                return true;
            } catch (AckFailedException e) {
                log.error("Ack failed to messageId " + messageId, e);
                return false;
            }
        } catch (AckFailedException e) {
            log.error("Ack failed to ackId " + ackId, e);
            return false;
        }
    }

    @Override
    public boolean ackSafe() {
        return ackSafe(ACKSAFE_TIMEOUT);
    }

    @Override
    public boolean ackSafe(long timeout) {
        if (config.getAckType() == ConsumerAckType.AUTO_CLIENT_ACK) {
            log.warn("This consumer has auto ack type. No need to explicitly ack.");
            return false;
        }

        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not ack.");
            return false;
        }

        if (lastSentServer != null) {
            try {
                lastSentServer.ackSafe(timeout);
                lastSentServer = null;
                return true;
            } catch (AckFailedException e) {
                log.error("Ack failed to server " + lastSentServer, e);
                return false;
            } catch (Exception e) {
                log.error("Ack failed", e);
                return false;
            }
        }
        return false;
    }

    private Message receiveImpl(boolean blocking) {
        if (this.status != Status.RUNNING) {
            log.warn("This consumer is not running, can not receive. Status="
                    + this.status);
            return null;
        }

        StompFrame tmpFrame = null;

        do {
            for (int idx = 0; idx < serverList.size(); idx++) {
                int serverIdx = (lastContactedServerIdx + 1 + idx)
                        % serverList.size();
                StompServerFetcher server = serverList.get(serverIdx);
                tmpFrame = server.receiveLast();
                if (tmpFrame != null) {
                    lastContactedServerIdx = serverIdx;
                    lastSentServer = server;
                    Message message = Utils.getMessageFromBytes(tmpFrame
                            .getBody().getBytes());
                    log.debug("Received message: " + message);
                    log.debug("received message-id: "
                            + message.getMessageId());
                    // set ackId hashed from hostname and port

                    if (message != null) {
                        StringBuilder clearText = new StringBuilder(
                                server.getHost() + ":");
                        clearText.append("" + server.getPort() + ":");
                        clearText.append(tmpFrame.getHeaders()
                                .get("message-id") + ":");

                        // bbansal: For backward compatibility connectionId is
                        // not a required field here.
                        String connectionId = (tmpFrame.getHeaders()
                                .containsKey("connection-id")) ? tmpFrame
                                .getHeaders().get("connection-id")
                                : Utils.NULL_STRING;
                        clearText.append(connectionId);

                        message.setAckId(Utils.encode(clearText.toString()));
                        message.setMessageProperties(tmpFrame.getHeaders());
                    }
                    return message;
                }
            }

            try {
                if (blocking) {
                    Thread.sleep(config.getReceiveSleepInterval());
                } else {
                    return null;
                }

            } catch (InterruptedException e) {
                log.debug("receiveImpl blocking timed out.");
                return null;
            }

        } while (blocking);

        return null; // make compiler happy
    }

    private void startAndRegisterConnection(HostParams aHost)
            throws InvalidDestinationException, BrokerConnectionFailedException {

        String threadName = aHost.getHost();

        StompServerFetcher connection = getStompServerFetcher(aHost, config);

        connection.refreshConnection();

        log.debug("opening connection for " + aHost.getHost() + " Port="
                + aHost.getPort());

        serverList.add(connection);
        Thread thread = new Thread(connection, threadName);
        thread.setDaemon(true);
        thread.start();

        if (!currentServers.containsKey(aHost)) {
            currentServers.put(aHost, new ArrayList<StompServerFetcher>());
        }

        // Add connection to currentServers connection list.
        currentServers.get(aHost).add(connection);

    }

    public StompServerFetcher getStompServerFetcher(HostParams aHost,
            ConsumerConfig aConfig) {
        return new StompServerFetcher(aHost.getHost(), aHost.getPort(), aConfig);
    }

    private void stopHostAndUnregisterConnection(HostParams aHost) {
        List<StompServerFetcher> tmpList = currentServers.get(aHost);
        // remove all
        if (tmpList != null) {
            for (StompServerFetcher server : tmpList) {
                serverList.remove(server);
                server.close();
                log.debug("Connection with the broker " + server.toString()
                        + " closed successfully");
            }
        }
        currentServers.remove(aHost);
        lastContactedServerIdx = -1; // reset this guy
    }

    private void validateConfigs(ConsumerConfig aConfig)
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

            if (config.getSubscriptionId() == null) {
                log.error("When consuming topics, subscription-id must be specified");
                throw new InvalidConfigException(
                        "When consuming for topics, subscription id must be specified");
            }

        }
    }

    public void refreshServers() {

        try {
            Set<HostParams> newServerSet = fetchHostList();
            StringBuffer logMessageBuffer = new StringBuffer(
                    "New server list includes");

            // find servers to stop and remove
            HashSet<HostParams> removedServers = new HashSet<HostParams>();
            for (HostParams aHost : currentServers.keySet()) {
                if (!newServerSet.contains(aHost)) {
                    removedServers.add(aHost);
                }
            }

            for (HostParams bHost : removedServers) {
                log.debug("Stopping " + bHost);
                this.stopHostAndUnregisterConnection(bHost);
            }

            // find servers to add
            for (HostParams aHost : newServerSet) {
                if (null == currentServers.get(aHost)) {
                    log.debug("Starting new broker connection with " + aHost);
                    try {
                        startAndRegisterConnection(aHost);
                    } catch (BrokerConnectionFailedException e) {
                        log.error("Failed to connect to " + aHost.toString()
                                + ".\n" + e.getStackTrace());
                    }
                }
                logMessageBuffer.append(", " + aHost);
            }

            log.debug("Dynamically refreshing Server List");
            log.debug(logMessageBuffer.toString());
        } catch (MalformedURLException me) {
            log.error(
                    "Incorrect host list url is provided. Abort refresh servers.",
                    me);
        } catch (IOException ie) {
            log.warn(
                    "IOException when trying to get server list. Aborting refresh servers.",
                    ie);
        } catch (Exception e) {
            log.warn(
                    "Exception when trying to get server list. Aborting refresh servers.",
                    e);
        }

    }

    public Set<HostParams> fetchHostList() throws MalformedURLException,
            IOException {
        return DynamicServerListGetter.fetchHostList(config
                .getDynamicServerListFetchURL());
    }

}

/**
 * Timer task that runs every specified interval and calls to refresh serverList
 * 
 * @author ameya
 */
class RefreshServerListTimerTask extends TimerTask {
    ConsumerImpl consumer;

    public RefreshServerListTimerTask(ConsumerImpl aConsumer) {
        consumer = aConsumer;
    }

    @Override
    public void run() {
        consumer.refreshServers();
    }
}
