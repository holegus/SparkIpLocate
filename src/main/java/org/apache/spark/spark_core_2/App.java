package org.apache.spark.spark_core_2;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class App {
	  public static void main(String[] args) {
		
		//Regex to catch IPv6 - the string will include at least one alpha character.
		Pattern p = Pattern.compile("[a-zA-Z]");
		//Regex to deal with csv geo-mapping file.
		//Pattern geomap = Pattern.compile("(\",\")");
		  
	    String locationFile = "/home/tom/IP2LOCATION-LITE-DB3.CSV";
	    String logFile = "/home/tom/rddtestweb.log";
	    //String logFile = "hdfs://cdh-nn2/user/tom/weblog/*"; 
	    //Should be some file on your system
	    
	    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.setLevel(Level.ERROR);
	        
	    JavaRDD<String> locationData = sc.textFile(locationFile);
	    
	    //Create RDD wit list of geolocation - for future compare web log IP number with begin & end IP numbers from this list.
	    JavaRDD<List<String>> geoLocation = locationData.map(new Function <String, List<String>>() {
	    	public List<String> call(String st) {
	    		//System.out.println(st.substring(1, st.length()-1));
	    		
	    		return Arrays.asList(st.substring(1, st.length()-1).split("(\",\")"));
	    	}
	    }); 
	   
	    //Read raw log file - may include comment with "#" at the line beginning. 
	    JavaRDD<String> logDataRaw = sc.textFile(logFile);
	    //Filter out "#" lines.
	    JavaRDD<String> logData = logDataRaw.filter(new Function<String,Boolean>() {
	    	public Boolean call(String s) {
	    		if (s.charAt(0) == '#') {
	    			return false;
	    		} else {
	    			return true;
	    		}
	    	}
	    });
	    
	    JavaRDD<List<String>> logLine = logData.map(new Function<String, List<String>>() {
	    	public List<String> call(String s) {
	    		return Arrays.asList(s.split(" ")[8]);
	    	}
	    }).distinct();
	    
	    JavaRDD<List<Long>> ipSplit = logLine.map(new Function<List<String>, List<Long>>() {
		   public List<Long> call(List<String> s) {
	    		List<Long> ipNumbers = new ArrayList<Long>();
	    		long summary = 0L;
			    for (String ip : s) {
	    			List<String> temp = Arrays.asList(ip.split("\\."));
	    			try {
	    				summary = 6777216 * (long)Integer.parseInt(temp.get(0)) + 65536 * (long)Integer.parseInt(temp.get(1)) + 256 * (long)Integer.parseInt(temp.get(2)) + (long)Integer.parseInt(temp.get(3));
	    			} catch (NumberFormatException e) {
	    				//  ip.split for IPv6 format will bring temp array with the single member - no "."
	    				// So temp.size check is necessary  
	    				if (temp.size() > 1) {
	    					// Double check if it is IPv6 with letters.
		    				Matcher m = p.matcher(temp.get(3));
	    					if (temp.get(3).contains(":") && !m.find()) {
	    						int index = temp.get(3).indexOf(":");	
	    						// Reformat the IP last octet - trim port number and recalculate IP number.
	    						String trimmed = temp.get(3).substring(0, index);
	    						summary = 6777216 * (long)Integer.parseInt(temp.get(0)) + 65536 * (long)Integer.parseInt(temp.get(1)) + 256 * (long)Integer.parseInt(temp.get(2)) + (long)Integer.parseInt(trimmed);
	    					} else {
	    						summary = 0L;
	    					}
	    				}
	    			}	
	    			ipNumbers.add(summary);
	    		}
			   return ipNumbers; 
	   	  }
	    }); 
	   
	    //System.out.println(geoLocation.);
	    //geoLocation.saveAsTextFile("/home/tom/Documents/GeoLocation.txt");
	    //ipSplit.saveAsTextFile("/home/tom/Documents/IPstring.txt"); 
	    sc.stop();
	  }
}