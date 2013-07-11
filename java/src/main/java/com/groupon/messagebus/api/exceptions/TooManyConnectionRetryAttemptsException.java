package com.groupon.messagebus.api.exceptions;

@SuppressWarnings("serial")
public class TooManyConnectionRetryAttemptsException extends BrokerConnectionFailedException {

    public TooManyConnectionRetryAttemptsException(String message) {
        super(message);
    }

    public TooManyConnectionRetryAttemptsException(Exception e) {
        super(e);
    }
}