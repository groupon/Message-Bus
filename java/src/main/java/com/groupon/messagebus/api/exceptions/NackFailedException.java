package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class NackFailedException extends MessageBusException{

    public NackFailedException(Exception e) {
        super(e);
    }

}
