package com.socket.server.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.Random;

public class SocketServer {
	
	public static void main(String[] args) {
		
		ServerSocket socServer = null;
	    String line;
	    DataInputStream is;
	    PrintStream os;
	    Socket clientSocket = null;
	    
    	Path path = FileSystems.getDefault()
    			.getPath("/home/mayank-ideata/setup/project/spark/gitproject/spark-learning/src/main/resources/data", 
    					"streamingtweets.txt");
	    List<String> lines = null;
		try {
			lines = Files.readAllLines(path,StandardCharsets.UTF_8);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	    try {
	    	socServer = new ServerSocket(9000);
	    	System.out.println("Socket opened");
	    	
	    	System.out.println("Total records read :" + lines.size());
	     }
	     catch (IOException e) {
	        System.out.println(e);
	     }   
	    
	    try {
	           clientSocket = socServer.accept();
	           System.out.println("Accepted client request from : " + clientSocket.getInetAddress() );
	           is = new DataInputStream(clientSocket.getInputStream());
	           os = new PrintStream(clientSocket.getOutputStream());

	           while (true) {
	        	 
	        	 //Pick a random line
	        	 int randomline = (int) (Math.random() * lines.size());
	        	 
	        	 System.out.println("Publishing " + lines.get(randomline));
	        	 os.println( lines.get(randomline) ); 
	        	 os.flush();
	        	 //Randomly sleep 1 - 3 seconds
	             Thread.sleep((long) (Math.random() * 3000));
	           }
	        }   
	    catch (Exception e) {
	           System.out.println(e);
	        }
	}
	

}

/* Run command
java -cp C:/Users/kumaran/Dropbox/V2Maestros/JavaWorkSpace/spark-big-data-analytics/target/classes com.v2maestros.spark.bda.common.SocketServer
*/
