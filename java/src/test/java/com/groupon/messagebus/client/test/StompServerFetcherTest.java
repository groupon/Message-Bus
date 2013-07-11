package com.groupon.messagebus.client.test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.ConsumerConfig;
import com.groupon.messagebus.api.exceptions.AckFailedException;
import com.groupon.messagebus.api.exceptions.KeepAliveFailedException;
import com.groupon.messagebus.api.exceptions.NackFailedException;
import com.groupon.messagebus.client.StompServerFetcher;
import com.groupon.stomp.Stomp;
import com.groupon.stomp.StompConnection;
import com.groupon.stomp.StompFrame;

public class StompServerFetcherTest extends TestCase{

    static Logger logger = Logger.getLogger(StompServerFetcherTest.class);
    StompFrame frame1;
    StompConnection connection;
    int connectionRequestCount = 0;
    boolean ran = false;
    ConsumerConfig config = new ConsumerConfig();

    @Before
    public void runOnce() {
        if (!ran) {
            BasicConfigurator.resetConfiguration();
            BasicConfigurator.configure();
            ran = true;
        }
    }

    void setDefaultConfigs(){
        config.setConnectionLifetime(300000);
        config.setDestinationName("jms.queue.TestQueue1");
    }

    @Before
    public void setUp() {
        setDefaultConfigs();
        frame1 = mock(StompFrame.class);

        when(frame1.getBody()).thenReturn("message1");
        when(frame1.getAction()).thenReturn("MESSAGE");
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put(Stomp.Headers.Message.MESSAGE_ID, "12345");
        headers.put("receipt", "1");
        when(frame1.getHeaders()).thenReturn(headers);
        connection = mock(StompConnection.class);

        try {

            when(connection.receive()).thenReturn(frame1);
            when(connection.receive(300000)).thenReturn(frame1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test0_fetchFromServerTest() throws Exception {
        System.out.println("Test: Check connection gets refreshed");

        StompServerFetcher fetcher = new StompServerFetcher("localhost", 1000, config, connection);

        Thread thread = new Thread(fetcher, "myTestThread0");
        thread.start();

        String message = null;
        while (message == null) {

            StompFrame tmpFrame = fetcher.receiveLast();
            if (tmpFrame != null) {
                message = tmpFrame.getBody();
            }
        }

        assertEquals("message1", message);
        fetcher.close();
        assertEquals(connection.isConnected(), false);
    } 

    @Test
    public void test1_connectionGetsRefreshedTest() throws Exception {
        System.out.println("Test: Check connection gets refreshed");

        config.setConnectionLifetime(2000);

        StompServerFetcher fetcher = new StompServerFetcher("localhost", 1000, config, connection);

        Thread thread = new Thread(fetcher, "myTestThread1");
        thread.start();

        String message = null;
        while (message == null) {

            StompFrame tmpFrame = fetcher.receiveLast();
            if (tmpFrame != null) {
                message = tmpFrame.getBody();

            }
        }

        Thread.sleep(3500);
        fetcher.ack();
        message = null;
        while (message == null) {
            StompFrame tmpFrame = fetcher.receiveLast();
            if (tmpFrame != null) {
                message = tmpFrame.getBody();
                fetcher.ack();
            }
        }

        fetcher.close();

    }

    @Test
    public void test2_clientAutoAckTest() throws Exception {
        System.out.println("Test: Check auto ack works");



        config.setAckType(ConsumerAckType.AUTO_CLIENT_ACK);

        StompServerFetcher fetcher = new StompServerFetcher("localhost", 1000, config, connection);

        Thread thread = new Thread(fetcher, "myTestThread0");
        thread.start();

        System.out.println("step2");

        String message = null;
        for (int i = 0; i < 2; i++) {
            while (message == null) {

                StompFrame tmpFrame = fetcher.receiveLast();
                if (tmpFrame != null) {
                    message = tmpFrame.getBody();
                    assertEquals("message1", message);
                }
            }
        }

        fetcher.close();
    }

    @Test
    public void test3_nack() throws NackFailedException, IOException {

        config.setAckType(ConsumerAckType.CLIENT_ACK);
        StompServerFetcher fetcher = new StompServerFetcher("localhost", 1000, config, connection);

        Thread thread = new Thread(fetcher, "myTestThread0");
        thread.start();

        while (fetcher.receiveLast() == null);
        fetcher.nack();
        verify(connection).nack(eq("12345"), anyString());;
        fetcher.close();
    }
    
    @Test
    public void test4_keepAlive() throws IOException, KeepAliveFailedException {

        StompServerFetcher fetcher = new StompServerFetcher("localhost", 1000, config, connection);

        Thread thread = new Thread(fetcher, "myTestThread0");
        thread.start();

        fetcher.keepAlive();
        verify(connection).keepAlive();
        fetcher.close();
    }

    @Test
    public void test5_ackSafe() throws IOException, AckFailedException, InterruptedException {
        StompFrame receiptFrame = mock(StompFrame.class);
        when(receiptFrame.getAction()).thenReturn(Stomp.Responses.RECEIPT);
        Map<String, String> headers = new Hashtable<String, String>();
        String receipt_id = java.util.UUID.nameUUIDFromBytes("12345".getBytes()).toString();
        headers.put(Stomp.Headers.Response.RECEIPT_ID,
                receipt_id);
        when(receiptFrame.getHeaders()).thenReturn(headers);

        when(connection.receive()).thenReturn(frame1, receiptFrame);
        when(connection.receive(300000)).thenReturn(frame1,receiptFrame);

        StompServerFetcher fetcher = new StompServerFetcher("localhost", 1000, config, connection);
      
        Thread thread = new Thread(fetcher, "myTestThread0");
        thread.start();
        while (fetcher.receiveLast() == null);
        fetcher.ackSafe(100);
        fetcher.close();

        verify(connection).ack("12345",null,null,null,receipt_id);;
    }
    @Test
    public void test6_ackSafeFail() throws IOException, AckFailedException, InterruptedException {
        StompServerFetcher fetcher = new StompServerFetcher("localhost", 1000, config, connection);
        
        Thread thread = new Thread(fetcher, "myTestThread0");
        thread.start();
        boolean ackFailed = false;

        while (fetcher.receiveLast() == null);
      //No mocks on receive hence should timeout
        try{
            fetcher.ackSafe(10);
        }
        catch(AckFailedException ae) {
            ackFailed = true;
        }
        fetcher.close();
        assertTrue(ackFailed);
    }
    public void test7() {
        Gson gson = new Gson();
        Map<String, String> map = new HashMap<String, String>();
        map.put("hello", "world");
        System.out.println(gson.toJson(new String("hello world")));
        System.out.println(gson.toJson(map));

    }
}
