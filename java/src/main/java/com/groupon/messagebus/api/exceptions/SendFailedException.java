package com.groupon.messagebus.api.exceptions;

public class SendFailedException extends MessageBusException{
    public SendFailedException(String message) {
        super(message);
    }

    public SendFailedException(Exception e) {
        super(e);
    }
}
