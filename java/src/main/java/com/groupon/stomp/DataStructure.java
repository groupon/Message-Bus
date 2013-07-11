package com.groupon.stomp;

public interface DataStructure {

    /**
     * @return The type of the data structure
     */
    byte getDataStructureType();
    boolean isMarshallAware();

}