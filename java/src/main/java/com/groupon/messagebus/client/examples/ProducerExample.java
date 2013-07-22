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
 * 
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;

import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.api.Producer;
import com.groupon.messagebus.api.ProducerConfig;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.MessageBusException;
import com.groupon.messagebus.client.ProducerImpl;

public class ProducerExample {

    public static void main(String[] args) throws InvalidConfigException, MessageBusException, FileNotFoundException, IOException {
        
        if (args.length < 1) {
            System.out.println("Usage: java -cp uber-mbus-client.jar com.groupon.messagebus.client.examples.ProducerExample config.properties");
            System.exit(1);
        }
        
        Properties properties = new Properties();
        properties.load(new FileInputStream(args[0]));

        BasicConfigurator.configure();

        ProducerConfig config = new ProducerConfig();

        // 61613 is default port for connecting over stomp. Here we either
        // provide direct broker name as host, or DNS name space

        HostParams host = new HostParams(properties.getProperty("server"), Integer.parseInt(properties.getProperty("port")));

        config.setBroker(host);
        config.setConnectionLifetime(300000);

        // chose between topic (one to many) or queue (one to one)
        config.setDestinationType(DestinationType.valueOf(properties.getProperty("dest_type")));
        config.setDestinationName(properties.getProperty("dest_name"));

        Producer producer = new ProducerImpl();

        producer.start(config);
        SecureRandom random = new SecureRandom();
        
        HashMap<String, String> headers = null;
        int priority = 4;
        if(properties.getProperty("priority")!=null)
        {
            priority = Integer.parseInt(properties.getProperty("priority"));
            headers = new HashMap<String, String>();
            headers.put("priority", "" + priority);

        }
        
        
        
        for (int i = 0; i < Integer.parseInt(properties.getProperty("msg_count")); i++) {
            String messageStr = "Count-" + i + " priority=" + priority + " random-data(" + new BigInteger(Integer.parseInt(properties.getProperty("msg_size")), random).toString(32) + ")";
            Message message = Message.createStringMessage( messageStr);

            try {
                producer.sendSafe(message, headers);
            } catch (Exception e) {
                e.printStackTrace();
            }
                                    
            System.out.println("Sent:" + message.getStringPayload());
        }

        producer.stop();
    }

}
