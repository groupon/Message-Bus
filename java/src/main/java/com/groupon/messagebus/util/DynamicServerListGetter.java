package com.groupon.messagebus.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.groupon.messagebus.api.HostParams;

public class DynamicServerListGetter {

    private static Logger log = Logger.getLogger(DynamicServerListGetter.class);
    private static final int MAX_READ_TIMEOUT = 5000;
    private static final int MAX_CONNECT_TIMEOUT = 1000;
    
    public static String buildDynamicServersURL(String hostname, int port) throws URISyntaxException{        
        URIBuilder builder =  new URIBuilder();
        builder.setHost(hostname);
        builder.setPort(port);
        builder.setPath("/jmx");
        builder.addParameter("command", "get_attribute");
        builder.addParameter("args", "org.hornetq:module=Core,type=Server ListOfBrokers");
        builder.setScheme("http");
        return builder.build().toASCIIString();        
    }
    
    protected static String fetch(String aURL) throws MalformedURLException, IOException {
        String content = null;
        URLConnection connection = null;
            
        connection = new URL(aURL).openConnection();
        connection.setConnectTimeout(MAX_CONNECT_TIMEOUT);
        connection.setReadTimeout(MAX_READ_TIMEOUT);
        Scanner scanner = new Scanner(connection.getInputStream());
        scanner.useDelimiter("\\Z");
        content = scanner.next();


        return content;
    }

    public static Set<HostParams> parseAndReturnHosts(String content) {
        Set<HostParams> serverSet = new HashSet<HostParams>();

        if (content == null || content.trim().length() == 0){
            return serverSet;
        }

        String[] servers = content.split(",");
        for (String host : servers) {                    
            String[] hostParam = host.split(":");
            serverSet.add(new HostParams(hostParam[0], Integer.parseInt(hostParam[1])));
        }

        return serverSet;
    }

    /**
     * Fetches XML from given URL, 
     * parses list of brokers and returns the brokers
     * as set of HostParams objects
     * @param aURL
     * @return
     * @throws IOException 
     * @throws MalformedURLException 
     */
    public static Set<HostParams> fetchHostList(String aURL) throws MalformedURLException, IOException {
        return parseAndReturnHosts(fetch(aURL));                              
    }

    public static void main(String[] args) throws MalformedURLException, IOException {


        String aURL = "http://localhost:18081/jmx?command=get_attribute&args=org.hornetq%3Amodule%3DCore%2Ctype%3DServer%20ListOfBrokers";
        BasicConfigurator.configure();


         Set<HostParams> hosts = parseAndReturnHosts(DynamicServerListGetter.fetch(aURL));


         if(hosts!=null){
             for(HostParams host: hosts){
                 System.out.println(host);
             }
         }

         if (hosts.size() == 0 || hosts == null)
             System.out.println("No servers found");


         }


}
