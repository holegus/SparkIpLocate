package org.apache.spark.spark_core_2;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class App {
	  public static void main(String[] args) {
		  
	    String locationFile = "/home/tom/IP2LOCATION-LITE-DB3.CSV";
	    //String logFile = "/home/tom/rddtestweb.log";
	    String logFile = "hdfs://cdh-nn2/user/tom/weblog/*"; 
	    //Should be some file on your system
	    
	    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.setLevel(Level.ERROR);
	    JavaRDD<String> locationData = sc.textFile(locationFile);
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
	    
	    // Java 7
	    JavaRDD<List<String>> logLine = logData.map(new Function<String, List<String>>() {
	    	public List<String> call(String s) {
	    		return Arrays.asList(s.split(" ")[8]);
	    	}
	    }).distinct();
	    
	   JavaRDD<List<Long>> ipSplit = logLine.map(new Function<List<String>, List<Long>>() {
		   public List<Long> call(List<String> s) {
	    		List<Long> ipNumbers = new ArrayList<Long>();
			    for (String ip : s) {
	    			List<String> temp = Arrays.asList(ip.split("\\."));
	    			long summary = 6777216 * (long)Integer.parseInt(temp.get(0)) + 65536 * (long)Integer.parseInt(temp.get(1)) + 256 * (long)Integer.parseInt(temp.get(2)) + (long)Integer.parseInt(temp.get(3));
	    			ipNumbers.add(summary);
	    		}
			   return ipNumbers; 
	   	}
	   }); 
	   	       
	    //System.out.println(logData.collect());
	    System.out.println(logLine.collect());
	    System.out.println(ipSplit.collect());
	    
	    sc.stop();
	  }
}