package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class ReceiveTimeoutException extends MessageBusException{
    public ReceiveTimeoutException(Exception e) {
        super(e);
    }

    public ReceiveTimeoutException(String message){
        super(message);
    }


}
