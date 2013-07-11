package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class BrokerConnectionCloseFailedException extends MessageBusException {
    public BrokerConnectionCloseFailedException(Exception e){
        super(e);
    }
    
    public BrokerConnectionCloseFailedException(String message){
        super(message);
    }
}
