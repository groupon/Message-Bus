package com.groupon.messagebus.client.test;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.Test;

import com.groupon.messagebus.api.Producer;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.api.ProducerConfig;
import com.groupon.messagebus.api.exceptions.BrokerConnectionCloseFailedException;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.api.exceptions.SendFailedException;
import com.groupon.messagebus.api.exceptions.TooManyConnectionRetryAttemptsException;
import com.groupon.messagebus.client.ProducerImpl;
import com.groupon.messagebus.thrift.api.MessageInternal;
import com.groupon.stomp.StompConnection;

public class ProducerImplTest extends TestCase {
    private ProducerConfig config = new ProducerConfig();
    private HostParams dummyHostParam = new HostParams("localhost", 61613);
    private String MESSAGE_ID = "id-1";
    private byte[] MESSAGE_DATA = "my sweeet message.".getBytes();
    private byte[] MESSAGE_DATA2 = "my failing message.".getBytes();

    void setDefaultProducerConfigs() {
        config = new ProducerConfig();
        config.setConnectionLifetime(300000);
        config.setDestinationName("jms.queue.TestQueue1");
        config.setBroker(dummyHostParam);
    }

    public void setUp() {
        setDefaultProducerConfigs();
        BasicConfigurator.configure();
    }

    private StompConnection mockStompConnection() throws TException,
            IOException, SendFailedException {
        StompConnection connection = mock(StompConnection.class);

        when(connection.isConnected()).thenReturn(true);

        // for simple send do nothing.
        doNothing().when(connection).send(config.getDestinationName(),
                getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA), null);

        // for simple sendSafe do Nothing.
        doNothing().when(connection).sendSafe(config.getDestinationName(),
                getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA), null);

        // for messageData2 throw receipt not found exception.
        doThrow(new SendFailedException("Receipt failed")).when(connection)
                .sendSafe(config.getDestinationName(),
                        getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA2), null);

        return connection;
    }
    
    private StompConnection mockStompConnectionNoMsgId() throws TException,
    IOException, SendFailedException {
        StompConnection connection = mock(StompConnection.class);

        when(connection.isConnected()).thenReturn(true);

        //for simple send do nothing.
        doNothing().when(connection).send(config.getDestinationName(),
        getMessageAsBytes(MESSAGE_DATA), null);

        // for simple sendSafe do Nothing.
        doNothing().when(connection).sendSafe(config.getDestinationName(),
        getMessageAsBytes(MESSAGE_DATA), null);

        // for messageData2 throw receipt not found exception.
        doThrow(new SendFailedException("Receipt failed")).when(connection)
        .sendSafe(config.getDestinationName(),
                getMessageAsBytes(MESSAGE_DATA2), null);

        return connection;
}   

    private byte[] getMessageAsBytes(String messageId, byte[] messageData)
            throws TException, UnsupportedEncodingException {
        MessageInternal message = Message.createBinaryMessage(messageId,
                messageData).getMessageInternal();

        TSerializer serializer = new TSerializer();
        byte[] bytes = serializer.serialize(message);
        return Base64.encodeBase64(bytes);
    }
    
    private byte[] getMessageAsBytes(byte[] messageData)
            throws TException, UnsupportedEncodingException {
        MessageInternal message = Message.createBinaryMessage(messageData).getMessageInternal();

        TSerializer serializer = new TSerializer();
        byte[] bytes = serializer.serialize(message);
        return Base64.encodeBase64(bytes);
    }

    @Test
    public void testSend() throws TException, SendFailedException, IOException,
            TooManyConnectionRetryAttemptsException, InvalidConfigException,
            InvalidStatusException, BrokerConnectionCloseFailedException {

        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);

        Message message = Message.createBinaryMessage(MESSAGE_ID, MESSAGE_DATA);

        producer.start(config);

        try {
            producer.send(message, null);
        } catch (TooManyConnectionRetryAttemptsException br) {
            // exception occurred, don't pass this test
            assertEquals(false, true);
        }

        verify(connection).send(config.getDestinationName(),
                getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA), null);

        producer.stop();

    }
    
    @Test
    public void testSendNoMsgId() throws TException, SendFailedException, IOException,
            TooManyConnectionRetryAttemptsException, InvalidConfigException,
            InvalidStatusException, BrokerConnectionCloseFailedException {

        StompConnection connection = mockStompConnectionNoMsgId();
        ProducerImpl producer = new ProducerImpl(connection);

        Message message = Message.createBinaryMessage(MESSAGE_DATA);

        producer.start(config);

        try {
            producer.send(message, null);
        } catch (TooManyConnectionRetryAttemptsException br) {
            // exception occurred, don't pass this test
            assertEquals(false, true);
        }
        
        TSerializer serializer = new TSerializer();
        byte[] bytes = serializer.serialize(message.getMessageInternal());

        verify(connection).send(config.getDestinationName(),
                Base64.encodeBase64(bytes), null);

        producer.stop();

    }

    @Test
    public void testSendSafeScheduledMessage() throws TException,
            SendFailedException, IOException,
            TooManyConnectionRetryAttemptsException, InvalidConfigException,
            InvalidStatusException, BrokerConnectionCloseFailedException {
        int scheduledDelayMs = 1000;
        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);

        Message message = Message.createBinaryMessage(MESSAGE_ID, MESSAGE_DATA);

        producer.start(config);

        Map<String, String> headers = new HashMap<String, String>();
        long time = System.currentTimeMillis();
        headers.put(ProducerImpl.SCHEDULED_MESSAGES_DELIVERY_TIME_MS, new Long(
                time + scheduledDelayMs).toString());

        producer.sendSafe(message, headers);

        verify(connection).sendSafe(config.getDestinationName(),
                getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA), headers);
        producer.stop();
    }
    
    @Test
    public void testSendSafeScheduledMessageNoMsgId() throws TException,
            SendFailedException, IOException,
            TooManyConnectionRetryAttemptsException, InvalidConfigException,
            InvalidStatusException, BrokerConnectionCloseFailedException {
        int scheduledDelayMs = 1000;
        StompConnection connection = mockStompConnectionNoMsgId();
        ProducerImpl producer = new ProducerImpl(connection);

        Message message = Message.createBinaryMessage(MESSAGE_DATA);

        producer.start(config);

        Map<String, String> headers = new HashMap<String, String>();
        long time = System.currentTimeMillis();
        headers.put(ProducerImpl.SCHEDULED_MESSAGES_DELIVERY_TIME_MS, new Long(
                time + scheduledDelayMs).toString());

        producer.sendSafe(message, headers);

        TSerializer serializer = new TSerializer();
        byte[] bytes = serializer.serialize(message.getMessageInternal());
        
        verify(connection).sendSafe(config.getDestinationName(),
                Base64.encodeBase64(bytes), headers);
        producer.stop();
    }

    @Test
    public void testSendSafe() throws UnsupportedEncodingException, TException,
            Exception {
        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);

        Message message = Message.createBinaryMessage(MESSAGE_ID, MESSAGE_DATA);

        producer.start(config);

        producer.sendSafe(message, null);

        verify(connection).sendSafe(config.getDestinationName(),
                getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA), null);
        producer.stop();
    }

    @Test
    public void testSendSafeError() throws TException, SendFailedException,
            IOException, TooManyConnectionRetryAttemptsException,
            InvalidConfigException, InvalidStatusException,
            BrokerConnectionCloseFailedException {
        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);
        Message message = Message
                .createBinaryMessage(MESSAGE_ID, MESSAGE_DATA2);

        producer.start(config);

        try {
            producer.sendSafe(message, null);
        } catch (SendFailedException e) {
            // make test pass on error
            assertEquals(true, true);
        }

        verify(connection, times(config.getPublishMaxRetryAttempts()))
                .sendSafe(config.getDestinationName(),
                        getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA2), null);
        producer.stop();
    }

    @Test
    public void testCheckConnectionRefresh()
            throws UnsupportedEncodingException, TException, Exception {
        config.setConnectionLifetime(200);
        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);

        // Add extra mock call to refreshServer.
        doNothing().when(connection).close();
        doNothing().when(connection).open(dummyHostParam.getHost(),
                dummyHostParam.getPort());
        doNothing().when(connection).connect(config.getUserName(),
                config.getPassword());

        producer.start(config);

        Thread.sleep(300);

        verify(connection, times(2)).open(dummyHostParam.getHost(),
                dummyHostParam.getPort());
        verify(connection, times(2)).connect(config.getUserName(),
                config.getPassword());
        verify(connection).close();

        producer.stop();
    }

    @Test
    public void testStatusTransition() throws SendFailedException, TException,
            IOException, TooManyConnectionRetryAttemptsException,
            InvalidConfigException, InvalidStatusException,
            BrokerConnectionCloseFailedException {
        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);

        assertTrue(producer.getStatus() == Producer.Status.INITIALIZED);

        Message message = Message.createBinaryMessage(MESSAGE_ID, MESSAGE_DATA);

        producer.start(config);
        assertTrue(producer.getStatus() == Producer.Status.RUNNING);

        producer.sendSafe(message, null);

        verify(connection).sendSafe(config.getDestinationName(),
                getMessageAsBytes(MESSAGE_ID, MESSAGE_DATA), null);
        producer.stop();
        assertTrue(producer.getStatus() == Producer.Status.STOPPED);

    }

    @Test
    public void testStatusTransitionBad1() throws SendFailedException,
            TException, IOException, TooManyConnectionRetryAttemptsException,
            InvalidConfigException, InvalidStatusException,
            BrokerConnectionCloseFailedException {
        // Verify that producer cannot send before started.
        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);

        Message message = Message.createBinaryMessage(MESSAGE_ID, MESSAGE_DATA);
        boolean invalidStatus = false;

        try {
            producer.sendSafe(message, null);
        } catch (InvalidStatusException e) {
            e.printStackTrace();
            invalidStatus = true;
        }

        assertTrue(invalidStatus);

    }

    @Test
    public void testStatusTransitionBad2() throws SendFailedException,
            TException, IOException, TooManyConnectionRetryAttemptsException,
            InvalidConfigException, InvalidStatusException,
            BrokerConnectionCloseFailedException {
        // Verify that producer cannot stop before started.
        StompConnection connection = mockStompConnection();
        ProducerImpl producer = new ProducerImpl(connection);

        boolean invalidStatus = false;
        try {

            producer.stop();
        } catch (InvalidStatusException e) {
            e.printStackTrace();
            invalidStatus = true;
        }

        assertTrue(invalidStatus);

    }
}
