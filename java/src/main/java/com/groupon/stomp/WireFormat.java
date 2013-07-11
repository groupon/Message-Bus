package com.groupon.stomp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Provides a mechanism to marshal commands into and out of packets
 * or into and out of streams, Channels and Datagrams.
 *
 * 
 */
public interface WireFormat {

    /**
     * Packet based marshaling 
     */
    ByteSequence marshal(Object command) throws IOException;

    /**
     * Packet based un-marshaling 
     */
    Object unmarshal(ByteSequence packet) throws IOException;

    /**
     * Stream based marshaling 
     */
    void marshal(Object command, DataOutput out) throws IOException;

    /**
     * Packet based un-marshaling 
     */
    Object unmarshal(DataInput in) throws IOException;

    /**
     * @param the version of the wire format
     */
    void setVersion(int version);

    /**
     * @return the version of the wire format
     */
    int getVersion();

}