package com.groupon.messagebus.api;

import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.api.exceptions.ReceiveTimeoutException;
import com.groupon.messagebus.api.Message;

/**
 * ClientSession to interact with Message Bus
 *
 * Implementation of this interface - ConsumerImpl provides following: Starts
 * new threads with all the configured brokers. Every time receive() is called,
 * this class round robins between these threads to access next message. In a
 * way its a load balancer for these threads.
 *
 * * Copyright (c) 2013, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
public interface Consumer {

    public enum Status{
        INITIALIZED, RUNNING, STOPPED
    }
    public Status getStatus();

    /**
     * Start the consumer with the provided ConsumerConfig class.
     *
     * @param config: A {@link ConsumerConfig} to configure this Consumer.
     * @return true for success otherwise false.
     */
    public boolean start(ConsumerConfig config) throws InvalidConfigException, InvalidStatusException;

    /**
     * Stop the consumer
     */
    public void stop() throws InvalidStatusException;

    /**
     * ReceiveImmediate checks if a value can be fetched without blocking
     * returns the value or null otherwise.
     *
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call.
     *
     * @return Messsage : {@link Message} received.
     */
    public Message receiveImmediate();

    /**
     * Blocking receive upto maximum of specified timeout milliseconds.
     *
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call.
     *
     * @param timeout: in ms
     *
     * @return Messsage : {@link Message} received.
     */
    public Message receive(long timeout) throws ReceiveTimeoutException;

    /**
     * Blocking receive.
     *
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call.
     *
     * @return Messsage : {@link Message} received.
     */
    public Message receive();

    /**
     * Ack the last received message.
     * 
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call
     * 
     * @return true or false if ack failed.
     */
    public boolean ack();

    /**
     * Ack the message given the ackId contained in Message.
     *
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call
     *
     * @param ackId AckID field in Message.
     *
     * @return true or false if ack failed.
     */

    public boolean ack(String ackId);

    /**
     * Ack the last message received.
     *
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call
     * The ackSafe() call blocks till it receives a confirmation from the server
     * Blocking ackSafe upto maximum of specified timeout milliseconds
     * @param ackId AckID field in Message.
     *
     * @return true or false if ack failed.
     * @throws InterruptedException 
     */

    public boolean ackSafe();


    /**
     * Ack the message given the ackId contained in Message.
     * 
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call
     * The ackSafe() call blocks till it receives a confirmation from the server
     * 
     * @param ackId AckID field in Message.
     *
     * @return true or false if ack failed.
     */
    public boolean ackSafe(String ackId );
    /**
     * Ack the last message received.
     *
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call
     * The ackSafe() call blocks till it receives a confirmation from the server
     *
     * @param ackId AckID field in Message.
     *
     * @return true or false if ack failed.
     * @throws InterruptedException
     */

    public boolean ackSafe(long timeout);


    /**
     * Ack the message given the ackId contained in Message.
     *
     * The user is required to ack() each message if he chooses ack_mode as
     * 'CLIENT' in ConsumerConfig. Setting ack_mode as 'AUTO_CLIENT' means this
     * consumer library will acknowledge internally after each receive() call
     * The ackSafe() call blocks till it receives a confirmation from the server
     *
     * Blocking ackSafe upto maximum of specified timeout milliseconds.
     * @param ackId AckID field in Message.
     *
     * @return true or false if ack failed.
     */
    public boolean ackSafe(String ackId, long timeout);

    /**
     * Nack the last received message
     *
     * @return true or false if nack failed.
     */

    public boolean nack();

    /**
     * Nack the message given the msgId contained in Message.
     *
     * @param msgId MsgID field in Message.
     *
     * @return true or false if nack failed.
     */

    public boolean nack(String msgId);

    /**
     * Keepalive sends a heart beat to all the servers
     * @return true or false if keepalive failed.
     */
    public boolean keepAlive();
}
