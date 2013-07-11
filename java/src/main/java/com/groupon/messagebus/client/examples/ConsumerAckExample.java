package com.groupon.messagebus.client.examples;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.groupon.messagebus.api.Consumer;
import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.ConsumerConfig;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.MessageBusException;
import com.groupon.messagebus.api.exceptions.ReceiveTimeoutException;
import com.groupon.messagebus.client.ConsumerImpl;
import com.groupon.messagebus.thrift.api.MessagePayloadType;
import com.groupon.stomp.StompConnection;

public class ConsumerAckExample {
     private static Logger log = Logger.getLogger(ConsumerAckExample.class);

     
    /**
     * @param args
     * @throws InterruptedException
     * @throws MessageBusException,
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    public static void main(String[] args) throws InterruptedException, MessageBusException, FileNotFoundException, IOException {
        if (args.length < 1) {
            System.out.println("Usage: java -cp uber-mbus-client.jar com.groupon.messagebus.client.examples.ProducerExample config.properties");
            System.exit(1);
        }
        
        Properties properties = new Properties();
        properties.load(new FileInputStream(args[0]));

        BasicConfigurator.configure();

        ConsumerConfig config = new ConsumerConfig();

        HostParams host = new HostParams(properties.getProperty("server"), Integer.parseInt(properties.getProperty("port")));
        Set<HostParams> hostsList = new HashSet<HostParams>();
        hostsList.add(host);
        config.setHostParams(hostsList);

        config.setConnectionLifetime(50000);

        if (null != properties.getProperty("dynamic_fetch_url")) {
            config.setDynamicServerListFetchURL("http://" + properties.getProperty("dynamic_fetch_url") + "/jmx?command=get_attribute&args=org.hornetq%3Amodule%3DCore%2Ctype%3DServer%20ListOfBrokers");
        }
        
        
        config.setDestinationType(DestinationType.valueOf(properties.getProperty("dest_type")));
        config.setDestinationName(properties.getProperty("dest_name"));
        
        config.setSubscriptionId(properties.getProperty("subscription_id"));
        
        config.setAckType(ConsumerAckType.CLIENT_ACK);

        BasicConfigurator.configure();

        Consumer con = new ConsumerImpl();
        try {
            con.start(config);
        } catch (InvalidConfigException e) {
            e.printStackTrace();
            return;
        }

        log.debug("connected");
        long startTime = System.currentTimeMillis();
        
        Object[] ackIds = new Object[Integer.parseInt(properties.getProperty("msg_count"))];
                
        int msg_rcvd = 0;
        for ( ; msg_rcvd < Integer.parseInt(properties.getProperty("msg_count")); msg_rcvd++) {
            log.debug("Waiting for connection.receive()");
            try{
            Message tmp = con.receive(Integer.parseInt(properties.getProperty("rcv_timeout")));
            if(tmp==null)
                continue;

            Object obj = tmp.getAckId();
            
            // Instead of acking lets save the ackIds in the array to be sent later via a new connection object.
            //con.ack((String) obj);
            ackIds[msg_rcvd] = obj; 

            MessagePayloadType type = tmp.getMessagePayloadType();
            log.debug("This message is received:");
            if (type == MessagePayloadType.BINARY) {
                log.debug(new String(tmp.getBinaryPayload()));
            } else if( type == MessagePayloadType.JSON){
                log.debug("json: "+new String(tmp.getJSONStringPayload()));

            }
        
            else { 
              log.debug(new String(tmp.getStringPayload()));
            }

            if ((msg_rcvd % 200) == 0) {
                double difference = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                if (difference != 0) {
                    double took = 200 * 1000 / (difference);
                    log.debug("Consumed 200 messages in " + difference + " ms at " + took
                            + "messages/second Total Consumed=" + msg_rcvd);
                }
            }
            }
            catch( ReceiveTimeoutException e ){
                log.debug("Received Timeout, may be out of messages !! \n", e);
            }     
        }
        
        // Start a new connection
        Consumer con2 = new ConsumerImpl();
        try {
            con2.start(config);
        } catch (InvalidConfigException e) {
            e.printStackTrace();
            return;
        }

        // Send saved ackIds using the new connection.
        for(int i = 0; i < msg_rcvd; i++) {
            if (ackIds[i] != null) {
              con2.ack((String) ackIds[i]);
            }
        }
        
        log.debug("connected");        
        // stop all connections
        con2.stop();
        con.stop();
    }

}
