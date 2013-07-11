package com.groupon.messagebus.api;

/**
 * Encapsulates broker host
 * 
 * @author ameya
 */
public class HostParams {
    private String host;
    private int port;

    public HostParams(String aHost, int aPort) {
        host = aHost;
        port = aPort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int hashCode() {
        String hostName = host + ":" + port;
        return hostName.hashCode();
    }

    @Override
    public boolean equals(Object o) {

        if (o instanceof HostParams) {
            HostParams other = (HostParams) o;

            return ((host + ":" + port).equalsIgnoreCase((other.getHost() + ":" + other.getPort())));
        }

        return false;
    }

}
