package com.groupon.messagebus.client.examples;
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

public class ConsumerExample {
     private static Logger log = Logger.getLogger(ConsumerExample.class);


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

        if( null != properties.getProperty("use_dynamic_servers")){
            config.setUseDynamicServerList(Boolean.parseBoolean(properties.getProperty("use_dynamic_servers")));
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
        for (int i = 0; i < Integer.parseInt(properties.getProperty("msg_count")); i++) {
            log.debug("Waiting for connection.receive()");
            try{
            Message tmp = con.receive(Integer.parseInt(properties.getProperty("rcv_timeout")));
            if(tmp==null)
                continue;

            Object obj = tmp.getAckId();
            con.ackSafe((String) obj);

            MessagePayloadType type = tmp.getMessagePayloadType();
            log.debug("This message is received:");
            if(tmp.getMessageProperties() != null && tmp.getMessageProperties().get("priority") != null)
                log.debug("priority=" + tmp.getMessageProperties().get("priority"));
            if (type == MessagePayloadType.BINARY) {
                log.debug(new String(tmp.getBinaryPayload()));
            } else if( type == MessagePayloadType.JSON){
                log.debug("json: "+new String(tmp.getJSONStringPayload()));

            }

            else { 
              log.debug(new String(tmp.getStringPayload()));
            }

            if ((i % 200) == 0) {
                double difference = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                if (difference != 0) {
                    double took = 200 * 1000 / (difference);
                    log.debug("Consumed 200 messages in " + difference + " ms at " + took
                            + "messages/second Total Consumed=" + i);
                }
            }
            }
            catch( ReceiveTimeoutException e ){
                log.debug("Received Timeout, may be out of messages !! \n", e);
            }
            
        }

        con.stop();
    }

}
