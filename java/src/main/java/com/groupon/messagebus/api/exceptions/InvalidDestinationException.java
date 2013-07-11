package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class InvalidDestinationException extends RuntimeException{
    public InvalidDestinationException(Exception e){
        super(e);
    }

    public InvalidDestinationException(String msg) {
        super(msg);
    }
}
