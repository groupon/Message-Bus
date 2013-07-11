package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class KeepAliveFailedException extends MessageBusException{

    public KeepAliveFailedException(Exception e) {
        super(e);
    }

}
