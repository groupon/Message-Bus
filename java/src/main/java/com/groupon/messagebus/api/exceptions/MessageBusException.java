package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class MessageBusException extends Exception {
    public MessageBusException(Exception e){
        super(e);
    }

    public MessageBusException(String message){
        super(message);
    }
}
