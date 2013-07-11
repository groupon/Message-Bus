package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class BrokerConnectionFailedException extends MessageBusException {
    public BrokerConnectionFailedException(String message) {
        super(message);
    }
    
    public BrokerConnectionFailedException(Exception e){
        super(e);
    }
}
