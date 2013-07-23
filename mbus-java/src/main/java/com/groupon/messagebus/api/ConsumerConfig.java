package com.groupon.messagebus.api;

/*
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
import java.util.Set;

import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;

public class ConsumerConfig {

    // Set of servers to consume from.
    private Set<HostParams> hostParamsSet = null;

    // Consumer destination information.
    private String destinationName;
    private DestinationType destinationType = DestinationType.QUEUE;
    private ConsumerAckType ackType = ConsumerAckType.CLIENT_ACK;

    // Connection lifetime. Servers drop consumer connections if inactive for
    // this time period.
    // also used for refreshing consumer connection internally.
    private long connectionLifetime = 300000;

    // For topics (only) different subscription id means different subscriptions
    // (each gets same message)
    // specifying same subscription ids means for that subscriptions data will
    // be load-balanced.
    // For queues setting subscription id is a noop and all consumers get
    // load-balanced.
    private String subscriptionId;

    // URL to fetch active server list dynamically.
    private String dynamicServerListFetchURL;
    private boolean useDynamicServerList = true;



    private String userName = "guest";
    private String password = "guest";

    private long receiveSleepInterval = 1;
    // Consumer maintains internal thread pool. This property sets the size of
    // the thread pool. Default value is 4. We recommend setting this less than
    // half the number of cores in the box
    private int threadPoolSize = 4;

    /******
     * 
     * @deprecated Number of threads per server is now always 1
     * 
     * @return Always 1.
     */
    @Deprecated
    public int getNumOfThreadsPerServer() {
        return 1;
    }

    /***
     * 
     * @deprecated Number of threads per server is now always 1. This function
     *             does nothing
     * 
     * @param numOfThreadsPerServer
     * 
     */
    @Deprecated
    public void setNumOfThreadsPerServer(int numOfThreadsPerServer) {

    }

    public boolean useDynamicServerList() {
        return useDynamicServerList;
    }

    public void setUseDynamicServerList(boolean useDynamicServerList) {
        this.useDynamicServerList = useDynamicServerList;
    }
    
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public Set<HostParams> getHostParams() {
        return hostParamsSet;
    }

    public void setHostParams(Set<HostParams> aHostList) {
        this.hostParamsSet = aHostList;
    }

    public long getConnectionLifetime() {
        return connectionLifetime;
    }

    public void setConnectionLifetime(long connectionLifetime) {
        this.connectionLifetime = connectionLifetime;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(DestinationType destinationType) {
        this.destinationType = destinationType;
    }

    public ConsumerAckType getAckType() {
        return ackType;
    }

    public void setAckType(ConsumerAckType ackType) {
        this.ackType = ackType;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDynamicServerListFetchURL() {
        return dynamicServerListFetchURL;
    }

    public void setDynamicServerListFetchURL(String dynamicServerListFetchURL) {
        this.dynamicServerListFetchURL = dynamicServerListFetchURL;
    }

    public long getReceiveSleepInterval() {
        return receiveSleepInterval;
    }

    public void setReceiveSleepInterval(long receiveSleepInterval) {
        this.receiveSleepInterval = receiveSleepInterval;
    }

}
