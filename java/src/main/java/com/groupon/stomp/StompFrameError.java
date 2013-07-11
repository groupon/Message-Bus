package com.groupon.stomp;

/**
 * Command indicating that an invalid Stomp Frame was received.
 * 
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompFrameError extends StompFrame {

    private final ProtocolException exception;

    public StompFrameError(ProtocolException exception) {
        this.exception = exception;
    }

    public ProtocolException getException() {
        return exception;
    }

}