package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class InvalidConfigException extends MessageBusException{
    public InvalidConfigException(String message){
        super(message);
    }
}


