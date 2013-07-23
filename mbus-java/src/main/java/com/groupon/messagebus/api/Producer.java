package com.groupon.messagebus.api;
/*
 * Copyright (c) 2013, Groupon, Inc.
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
import java.util.Map;

import com.groupon.messagebus.api.exceptions.BrokerConnectionCloseFailedException;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.api.exceptions.SendFailedException;
import com.groupon.messagebus.api.exceptions.TooManyConnectionRetryAttemptsException;

public interface Producer {
    public enum Status{
        INITIALIZED, RUNNING, STOPPED
    }
    public Status getStatus();

  /**
   * Start producer, this opens threads
   */
  public void start(ProducerConfig config) throws InvalidConfigException, TooManyConnectionRetryAttemptsException, InvalidStatusException;

  /**
   * Provides convenient API to refresh connection, in case connection with
   * the broker breaks during send or any other operation.
   *
   * @throws TooManyConnectionRetryAttemptsException
   *
   */
  public void refreshConnection() throws TooManyConnectionRetryAttemptsException;

  /**
   * Stop producer, throws exception
   */
  public void stop() throws BrokerConnectionCloseFailedException, InvalidStatusException;

  /**
   * Fire and forget send. Fast (1500+ QPS) but less reliable way of sending
   * data to the broker.
   * <p/>
   * The Producer thread sends data on the server connection and returns
   * instantly The Message server might not have persisted this message to any
   * durable format On server restarts/failure the user will lose messages.
   * This should be used when you have very high load and losing few messages
   * will not cause much harm.
   *
   * @param message : A {@link Message} to send.
   */
  public void send(Message message) throws TooManyConnectionRetryAttemptsException, SendFailedException;

  /**
   * Fire and forget send. Fast (1500+ QPS) but less reliable way of sending
   * data to the broker.
   * <p/>
   * The Producer thread sends data on the server connection and returns
   * instantly The Message server might not have persisted this message to any
   * durable format On server restarts/failure the user will lose messages.
   * This should be used when you have very high load and losing few messages
   * will not cause much harm.
   *
   * @param message : A {@link Message} to send.
   * @param headers : A Map<String, String> for headers to send along with the messages.
   */
  public void send(Message message, Map<String, String> headers) throws TooManyConnectionRetryAttemptsException, SendFailedException;

  /**
   * Fire and forget send. Fast (1500+ QPS) but less reliable way of sending
   * data to the broker, along with queue/topic name to send data to
   * <p/>
   * The Producer thread sends data on the server connection and returns
   * instantly The Message server might not have persisted this message to any
   * durable format On server restarts/failure the user will lose messages.
   * This should be used when you have very high load and losing few messages
   * will not cause much harm.
   *
   * @param message : A {@link Message} to send.
   * @param headers : A Map<String, String> for headers to send along with the messages.
   */
  public void send(Message message, String destinationName, Map<String, String> headers) throws TooManyConnectionRetryAttemptsException, SendFailedException;


  /**
   * Safe Send with Receipt.
   * <p/>
   * Moderately fast (150+ QPS) way of publishing data to message server. The
   * Producer thread sends data on the server connection and waits for server
   * to return a message receipt acknowledgment.
   * <p/>
   * Guarantees that messages are not lost for durable and persistent queues.
   *
   * @param message : A {@link Message} to send.
   */
  public void sendSafe(Message message) throws TooManyConnectionRetryAttemptsException, SendFailedException;

  /**
   * Safe Send with Receipt.
   * <p/>
   * Moderately fast (150+ QPS) way of publishing data to message server. The
   * Producer thread sends data on the server connection and waits for server
   * to return a message receipt acknowledgment.
   * <p/>
   * Guarantees that messages are not lost for durable and persistent queues.
   *
   * @param message : A {@link Message} to send.
   * @param headers : A Map<String, String> for headers to send along with the messages.
   */
  public void sendSafe(Message message, Map<String, String> headers) throws TooManyConnectionRetryAttemptsException, SendFailedException;


  /**
   * Safe Send with Receipt along with queue/topic name
   * <p/>
   * Moderately fast (150+ QPS) way of publishing data to message server. The
   * Producer thread sends data on the server connection and waits for server
   * to return a message receipt acknowledgment.
   * <p/>
   * Guarantees that messages are not lost for durable and persistent queues.
   *
   * @param message : A {@link Message} to send.
   * @param headers : A Map<String, String> for headers to send along with the messages.
   */
  public void sendSafe(Message message, String destinationName, Map<String, String> headers) throws TooManyConnectionRetryAttemptsException, SendFailedException;
  
  
}
