package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class InvalidStatusException extends RuntimeException {

    public InvalidStatusException(String message) {
        super(message);
    }

}
