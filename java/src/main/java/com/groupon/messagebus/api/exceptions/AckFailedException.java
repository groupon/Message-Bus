package com.groupon.messagebus.api.exceptions;


@SuppressWarnings("serial")
public class AckFailedException extends MessageBusException {
    public AckFailedException(Exception e){
        super(e);
    }

    public AckFailedException(String message) {
        super(message);
    }
}