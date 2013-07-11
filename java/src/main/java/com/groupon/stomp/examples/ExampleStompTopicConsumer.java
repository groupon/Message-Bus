package com.groupon.stomp.examples;

import java.util.HashMap;

import com.groupon.stomp.Stomp.Headers.Subscribe;
import com.groupon.stomp.Stomp;
import com.groupon.stomp.StompConnection;
import com.groupon.stomp.StompFrame;

public class ExampleStompTopicConsumer {
    public static void main(String []args) throws Exception{               
        stayConnected();

    }
    private static void stayConnected() throws Exception{
        String queueName = "jms.topic.TestQueue2";
        
        StompConnection connection = new StompConnection();
        connection.open("localhost", 61613);        
        connection.connect("rocketman", "rocketman", "client-2");        
        System.out.println(System.currentTimeMillis());         
        HashMap<String,String> headers = new HashMap<String,String>();
        headers.put("durable-subscriber-name", "test2");
        headers.put("id", "id2");
        headers.put("client-id" , "client-2");

        connection.subscribe(queueName, Subscribe.AckModeValues.CLIENT, headers);
        
        try{
        for(int i=0; i< 100000; i++){
            StompFrame message = connection.receive();
            System.out.println("Message  " + message.getBody());
            connection.ack(message.getHeaders().get(Stomp.Headers.Message.MESSAGE_ID), null, null, null, null);
        }
        }catch (Exception e){

            System.out.println(System.currentTimeMillis());
            e.printStackTrace();
            connection.close();
            stayConnected();
        }

        connection.disconnect();
    }

}