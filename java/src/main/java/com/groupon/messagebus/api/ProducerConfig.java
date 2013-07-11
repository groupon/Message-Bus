package com.groupon.messagebus.api;

import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;

public class ProducerConfig {

    private HostParams broker;
    private long connectionLifetime = 300000;
    private String destinationName;
    private DestinationType destinationType = DestinationType.QUEUE;
    private String userName = "rocketman";
    private String password = "rocketman";
    private int publishMaxRetryAttempts = 3;
    private boolean verboseLog = false;

    public boolean isVerboseLog() {
        return verboseLog;
    }

    public void setVerboseLog(boolean verboseLog) {
        this.verboseLog = verboseLog;
    }

	public int getPublishMaxRetryAttempts() {
        return publishMaxRetryAttempts;
    }

    public void setPublishMaxRetryAttempts(int publishMaxRetryAttempts) {
        this.publishMaxRetryAttempts = publishMaxRetryAttempts;
    }

    public HostParams getBroker() {
        return broker;
    }

    public void setBroker(HostParams broker) {
        this.broker = broker;
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

}
