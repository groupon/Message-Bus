package com.groupon.messagebus.client.test;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import junit.framework.TestCase;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.groupon.messagebus.api.Consumer;
import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.ConsumerConfig;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.exceptions.AckFailedException;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidDestinationException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.api.exceptions.MessageBusException;
import com.groupon.messagebus.api.exceptions.ReceiveTimeoutException;
import com.groupon.messagebus.client.ConsumerImpl;
import com.groupon.messagebus.client.StompServerFetcher;
import com.groupon.messagebus.client.Utils;
import com.groupon.messagebus.thrift.api.MessageInternal;
import com.groupon.messagebus.thrift.api.MessagePayload;
import com.groupon.messagebus.thrift.api.MessagePayloadType;
import com.groupon.messagebus.util.DynamicServerListGetter;
import com.groupon.messagebus.api.Message;
import com.groupon.stomp.Stomp;
import com.groupon.stomp.StompConnection;
import com.groupon.stomp.StompFrame;

public class ConsumerImplTest extends TestCase {
    ConsumerConfig config;
    static Logger logger = Logger.getLogger(ConsumerImpl.class);

    StompFrame frame1;
    StompConnection connection;
    int connectionRequestCount = 0;
    boolean ran = false;
    boolean validConfig = true;
    private int sleep_interval = 0;
    private HostParams dummyHostParam = new HostParams("dummy", 61613);

    void setDefaultConsumerConfigs() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
        sleep_interval = 0;
        config = new ConsumerConfig();
        config.setConnectionLifetime(300000);
        config.setUseDynamicServerList(false);
        config.setDestinationName("jms.queue.TestQueue1");
        Set<HostParams> hostSet = new HashSet<HostParams>();
        hostSet.add(dummyHostParam);
        config.setHostParams(hostSet);
    }

    public void setUp() {
        setDefaultConsumerConfigs();

        try {
            frame1 = mock(StompFrame.class);
            MessageInternal message = new MessageInternal();
            message.setMessageId("id-1");
            MessagePayload payload = new MessagePayload(
                    MessagePayloadType.BINARY);
            payload.setBinaryPayload("message1".getBytes());
            message.setPayload(payload);

            TSerializer serializer = new TSerializer();
            byte[] bytes = serializer.serialize(message);
            bytes = Base64.encodeBase64(bytes);

            when(frame1.getBody()).thenReturn(new String(bytes));
            when(frame1.getAction()).thenReturn("Message");
            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("message-id", "12345");
            headers.put("receipt", "1");
            when(frame1.getHeaders()).thenReturn(headers);

            connection = mock(StompConnection.class);

            when(connection.isConnected()).thenReturn(true);

            when(connection.receive()).thenAnswer(new Answer() {
                public Object answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    Object mock = invocation.getMock();
                    try {
                        Thread.sleep(sleep_interval);
                        System.out.println("Sleeping...");
                    } catch (InterruptedException e) {
                        // ignore error here
                    }

                    return frame1;
                }
            });

            when(connection.receive(300000)).thenAnswer(new Answer() {
                public Object answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    Object mock = invocation.getMock();
                    try {
                        Thread.sleep(sleep_interval);
                        System.out.println("Sleeping...");
                    } catch (InterruptedException e) {
                        // ignore error here
                    }

                    return frame1;
                }
            });

        } catch (Exception e) {
            throw new RuntimeException("setup failed with exception", e);
        }
    }

    private ConsumerImpl mockConsumerImpl(ConsumerConfig config) {
        ConsumerImpl mockedImpl = spy(new ConsumerImpl());
        mockRefreshConnection(mockedImpl, config);
        return mockedImpl;
    }

    private ConsumerImpl badConsumerImpl(ConsumerConfig config)
            throws TException, UnknownHostException, IOException,
            MessageBusException {
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }

        ConsumerImpl mockedImpl = spy(new ConsumerImpl());
        newMockRefreshConnection(mockedImpl, config);
        return mockedImpl;
    }

    private void newMockRefreshConnection(ConsumerImpl mockedImpl,
            ConsumerConfig config) throws TException, UnknownHostException,
            IOException, MessageBusException {

        HostParams dummyHostParam = new HostParams("dummy", 61613);

        StompConnection connection = spy(new StompConnection());

        doNothing().when(connection).open(dummyHostParam.getHost(),
                dummyHostParam.getPort());
        doNothing().when(connection).connect(config.getUserName(),
                config.getPassword(), config.getSubscriptionId());
        when(connection.isConnected()).thenReturn(false);

        doNothing().when(connection).sendFrame(anyString());
        StompFrame frame2 = spy(new StompFrame());
        when(frame2.getAction()).thenReturn("ERROR");

        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("message-id", "12345");
        headers.put("receipt", "1");
        when(frame1.getHeaders()).thenReturn(headers);
        doReturn(frame2).when(connection).receive();

        StompServerFetcher mockedStompServerFetcher = new StompServerFetcher(
                dummyHostParam.getHost(), dummyHostParam.getPort(), config,
                connection);

        when(mockedImpl.getStompServerFetcher(dummyHostParam, config))
                .thenReturn(mockedStompServerFetcher);
    }

    private void mockRefreshConnection(ConsumerImpl mockedImpl,
            ConsumerConfig config) {
        StompServerFetcher mockedStompServerFetcher = new StompServerFetcher(
                dummyHostParam.getHost(), dummyHostParam.getPort(), config,
                connection);
        // for prefetch thread to handle control to main thread - ackSafe
        when(mockedImpl.getStompServerFetcher(dummyHostParam, config))
                .thenReturn(mockedStompServerFetcher);
    }

    @Test
    public void test0_BasicConnectAndGetMessage()
            throws InvalidConfigException, MessageBusException {
        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);

        Message message = consumer.receive();
        assertEquals("message1", new String(message.getBinaryPayload()));
        assertTrue(message.getMessageProperties() != null && message.getMessageProperties().get("message-id") == "12345");
        consumer.stop();
    }

    @Test
    public void test1_BlockingReceiveTest() throws InvalidConfigException,
            MessageBusException {
        sleep_interval = 200;

        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);

        String message = new String(consumer.receive().getBinaryPayload());
        consumer.ack();
        assertEquals("message1", message);
        consumer.stop();
    }

    @Test
    public void test2_ReceiveImmediateTest() throws InvalidConfigException,
            InvalidStatusException {
        sleep_interval = 200;

        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);

        Message message = consumer.receiveImmediate();
        consumer.ack();
        assertEquals(null, message);
        consumer.stop();
    }

    @Test
    public void test3_ReceiveTimeOutTest() throws InvalidConfigException,
            InvalidStatusException {
        sleep_interval = 150;
        boolean timedOut = false;

        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);

        // times out

        Message message = null;
        try {
            message = consumer.receive(100);
            consumer.ack();
        } catch (ReceiveTimeoutException e) {
            timedOut = true;
        }

        assertTrue(timedOut);
        consumer.stop();
    }

    @Test
    public void test3_NoReceiveTimeOutTest() throws InvalidConfigException,
            MessageBusException {
        sleep_interval = 150;

        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);

        Message message = null;
        try {
            message = consumer.receive(300);
            consumer.ack();
        } catch (ReceiveTimeoutException e) {
            fail("Unexpected receiveTimeoutException seen.");
        }

        assertEquals("message1", new String(message.getBinaryPayload()));
        consumer.stop();
    }

    @Test
    public void test3_MultipleReceiveTimeOutTest()
            throws InvalidConfigException, InterruptedException,
            MessageBusException {
        sleep_interval = 300;
        boolean timedOut = false;

        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);

        // times out

        Message message = null;
        try {
            message = consumer.receive(100);
            consumer.ack();
        } catch (ReceiveTimeoutException e) {
            timedOut = true;
        }

        assertTrue(timedOut);

        try {
            message = consumer.receive(150);
            consumer.ack();
        } catch (ReceiveTimeoutException e) {
            timedOut = true;
        }
        assertTrue(timedOut);

        Thread.sleep(100);

        try {
            message = consumer.receiveImmediate();
            consumer.ack();
        } catch (Exception e) {
            fail("Unexpected receiveTimeoutException seen.");
        }
        assertNotNull(message);
        assertEquals("message1", new String(message.getBinaryPayload()));
        consumer.stop();
    }

    @Test
    public void test4_ValidConfigsTest() throws InvalidStatusException {
        config.setDestinationType(DestinationType.TOPIC);
        config.setDestinationName("jms.queue.test1");
        ConsumerImpl consumer = mockConsumerImpl(config);

        boolean invalidConfigExceptionSeen = false;
        // negative test
        try {
            consumer.start(config);
            consumer.stop();
        } catch (InvalidConfigException e) {
            invalidConfigExceptionSeen = true;

        }
        assertTrue(invalidConfigExceptionSeen);

    }

    @Test
    public void test5_ValidConfigsTest2() {
        config.setDestinationType(DestinationType.TOPIC);
        config.setDestinationName("jms.topic.test1");
        ConsumerImpl consumer = mockConsumerImpl(config);

        boolean invalidConfigExceptionSeen = false;
        try {
            consumer.start(config);
            consumer.stop();
        } catch (InvalidConfigException e) {
            invalidConfigExceptionSeen = true;
        } catch (InvalidStatusException e) {

        }
        assertTrue(invalidConfigExceptionSeen);
    }

    @Test
    public void test6_ValidConfigsTest2() throws InvalidStatusException {
        config.setDestinationType(DestinationType.TOPIC);
        config.setDestinationName("jms.topic.test1");
        config.setSubscriptionId("merchant-center");
        ConsumerImpl consumer = mockConsumerImpl(config);

        try {
            consumer.start(config);
        } catch (InvalidConfigException e) {
            fail("Unexpected invalidConfig Exception.");
        }
        consumer.stop();
    }

    @Test
    public void FIXME_test7_checkConnectionGetsRefreshed()
            throws InvalidConfigException, InterruptedException,
            InvalidStatusException, MalformedURLException, IOException {
        config.setDynamicServerListFetchURL("http://dummy.test");
        config.setConnectionLifetime(200);

        ConsumerImpl consumer = mockConsumerImpl(config);

        // set fetchHostList to return dummy value back.
        Set<HostParams> dummyHostParams = new HashSet<HostParams>();
        dummyHostParams.add(dummyHostParam);

        doReturn(dummyHostParams).when(consumer).fetchHostList();
        // Add extra mock call to refreshServer.
        doNothing().when(consumer).refreshServers();

        consumer.start(config);

        Thread.sleep(300);

        verify(consumer).refreshServers();

        consumer.stop();
    }

    @Test
    public void test8_checkMessageAckID() throws ReceiveTimeoutException,
            InvalidStatusException {
        ConsumerImpl consumer = mockConsumerImpl(config);

        try {
            consumer.start(config);
            Message message = consumer.receive(300);
            consumer.ack(message.getAckId());
        } catch (InvalidConfigException e) {
            fail("Unexpected invalidConfig Exception.");
        }
        consumer.stop();
    }

    @Test
    public void test10_StatusTransitionTest() throws InvalidStatusException,
            InvalidConfigException {
        ConsumerImpl consumer = mockConsumerImpl(config);
        assertTrue(consumer.getStatus() == Consumer.Status.INITIALIZED);
        consumer.start(config);
        assertTrue(consumer.getStatus() == Consumer.Status.RUNNING);

        try {
            consumer.receive(300);
            consumer.ack();
        } catch (ReceiveTimeoutException e) {
        }
        assertTrue(consumer.getStatus() == Consumer.Status.RUNNING);
        consumer.stop();
        assertTrue(consumer.getStatus() == Consumer.Status.STOPPED);
    }

    @Test
    public void test11_StatusTransitionBad1() throws InvalidConfigException {

        // Must not stop a stopped consumer
        ConsumerImpl consumer = mockConsumerImpl(config);
        assertTrue(consumer.getStatus() == Consumer.Status.INITIALIZED);
        consumer.start(config);
        assertTrue(consumer.getStatus() == Consumer.Status.RUNNING);
        boolean invalidStatus = false;
        try {
            consumer.start(config);
        } catch (InvalidStatusException e) {
            e.printStackTrace();
            invalidStatus = true;

        }
        assertTrue(invalidStatus);
        consumer.stop();
    }

    @Test
    public void test12_StatusTransitionBad2() throws InvalidConfigException {
        // Must be running when the consumer stops
        ConsumerImpl consumer = mockConsumerImpl(config);
        assertTrue(consumer.getStatus() == Consumer.Status.INITIALIZED);
        boolean invalidStatus = false;
        try {
            consumer.stop();
        } catch (InvalidStatusException e) {
            e.printStackTrace();
            invalidStatus = true;

        }

        assertTrue(invalidStatus);
    }

    @Test
    public void test12_StatusTransitionBad3() throws InvalidConfigException {
        // Must be running when the consumer stops
        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);

        consumer.stop();
        assertEquals(consumer.receive(), null);
    }

    @Test
    public void test13_KeepAlive() throws InvalidConfigException,
            InvalidStatusException, IOException {
        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);
        consumer.receive();
        boolean keepAlive = consumer.keepAlive();
        verify(connection).keepAlive();
        assertTrue(keepAlive);
        consumer.stop();
    }

    @Test
    public void test14_NackLastMessage() throws InvalidConfigException,
            InvalidStatusException, IOException {

        // verify if nack sent for last sent message
        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);
        consumer.receive();
        consumer.nack();
        consumer.stop();
        verify(connection).nack(eq("12345"), anyString());
    }

    @Test
    public void test15_NackMessage() throws InvalidConfigException,
            InvalidStatusException, IOException {

        // verify if nack sent correctly for a message and sent to the right
        // broker
        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);
        Message receivedMessage = consumer.receive();
        consumer.nack(receivedMessage.getAckId());
        verify(connection).nack(eq("12345"), anyString());
        consumer.stop();
    }

    public void test16_ackSafeLastMessage() throws InvalidConfigException,
            InvalidStatusException, IOException, AckFailedException,
            InterruptedException {

        ConsumerImpl consumer = mockConsumerImpl(config);
        StompFrame receiptFrame = mock(StompFrame.class);
        when(receiptFrame.getAction()).thenReturn(Stomp.Responses.RECEIPT);
        Map<String, String> headers = new Hashtable<String, String>();
        String receipt_id = java.util.UUID.nameUUIDFromBytes("12345".getBytes()).toString();
        headers.put(Stomp.Headers.Response.RECEIPT_ID,
                receipt_id);
        when(receiptFrame.getHeaders()).thenReturn(headers);
        when(connection.receive(300000)).thenReturn(frame1, receiptFrame);

        StompServerFetcher mockedStompServerFetcher = spy(new StompServerFetcher(
                dummyHostParam.getHost(), dummyHostParam.getPort(), config,
                connection));

        when(consumer.getStompServerFetcher(dummyHostParam, config))
                .thenReturn(mockedStompServerFetcher);

        consumer.start(config);
        consumer.receive();
        boolean result = consumer.ackSafe();
        consumer.stop();
        verify(mockedStompServerFetcher).ackSafe(1000);
        verify(connection).ack("12345",null,null,null,receipt_id);
        assertTrue(result);
    }

    public void test17_ackSafeTimeOut() throws InvalidConfigException,
            InvalidStatusException, IOException, AckFailedException,
            InterruptedException {
        ConsumerImpl consumer = mockConsumerImpl(config);
        StompServerFetcher mockedStompServerFetcher = spy(new StompServerFetcher(
                dummyHostParam.getHost(), dummyHostParam.getPort(), config,
                connection));
        when(consumer.getStompServerFetcher(dummyHostParam, config))
                .thenReturn(mockedStompServerFetcher);

        consumer.start(config);
        consumer.receive();
        boolean result = consumer.ackSafe(10);
        assertTrue(!result);
        consumer.stop();
    }
    
    @Test
    public void test18_ackLastMessage() throws InvalidConfigException, InvalidStatusException, IOException {
        ConsumerImpl consumer = mockConsumerImpl(config);
        consumer.start(config);
        consumer.receive();
        boolean result = consumer.ack();
        verify(connection).ack("12345", null, null, null, null);
        consumer.stop();
        assertTrue(result);
    }
    
    @Test
    public void test19_useDynamicServers() throws InvalidConfigException, InvalidStatusException, IOException, URISyntaxException {
        ConsumerConfig config1 = new ConsumerConfig();
        config1.setConnectionLifetime(300000);
        config1.setDestinationName("jms.queue.TestQueue1");
        config1.setUseDynamicServerList(true);
        Set<HostParams> hostSet = new HashSet<HostParams>();
        hostSet.add(dummyHostParam);
        config1.setHostParams(hostSet);
        
        ConsumerImpl consumer = mockConsumerImpl(config1);
        Mockito.doReturn(new HashSet<HostParams>()).when(consumer).fetchHostList();
        consumer.start(config1);
        consumer.stop();
        assertEquals(config1.getDynamicServerListFetchURL(), DynamicServerListGetter.buildDynamicServersURL("dummy", 8081));
    }
    
    @Test
    public void test19_dontUseDynamicServers() throws InvalidConfigException, InvalidStatusException, IOException {
        ConsumerConfig config1 = new ConsumerConfig();
        config1.setConnectionLifetime(300000);
        config1.setDestinationName("jms.queue.TestQueue1");
        config1.setUseDynamicServerList(false);
        Set<HostParams> hostSet = new HashSet<HostParams>();
        hostSet.add(dummyHostParam);
        config1.setHostParams(hostSet);
        
        ConsumerImpl consumer = mockConsumerImpl(config1);
        Mockito.doReturn(new HashSet<HostParams>()).when(consumer).fetchHostList();
        consumer.start(config1);
        consumer.stop();
        verify(consumer, never()).fetchHostList();
    }
}
