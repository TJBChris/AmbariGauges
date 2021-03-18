package com.bentperception.ArduinoGauges;

//import java.io.Console;
import java.io.BufferedReader;
//import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
//import java.net.HttpURLConnection;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.HashMap;
//import java.util.Iterator;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.SSLSession;

import gnu.io.CommPortIdentifier; 
import gnu.io.SerialPort;

import org.apache.commons.codec.binary.Base64;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

// Query Ambari for selected values and send the results
// to the serial port specified on the command line.

// Sample command line: -port:COM1: -ambariURL:http://ambarhost.someplace.com:8443 -sleepTime:15000
// (sleepTime is in ms)
// This will prompt for username/password on Init.

public class AmbariGauges {

	private static BufferedReader input;
	private static OutputStream output;
	private static int bufferSeconds = 600;
	// Check engine = steady = errors, flashing slow = data, mgmt and mon svcs down, fast flash = can't reach ambari
	private static int clusterPct = 0; // Speed
	private static int clusterJobs = 0; // Tach
	private static int alertCount = 3; // Check Engine
	private static int hdfsPct = 0; // Fuel
	private static int hdfsAlerts = 0; // OIL
	private static int yarnAlerts = 0; // Volts
	private static int monAlerts; // AT OIL TEMP
	private final static String USER_AGENT = "HadoopGauges/1.5";
	//private static String[] jsonCalls = {"namenode","resourcemanager","yarn","hdfs","nagios","hive","mr2","hbase","metricsCPU"};
	private static String[] jsonCalls = {"namenode","resourcemanager","yarn","hdfs","nagios","hive","mr2","metricsCPU"};
	private static int error = 0;
	
	// gear - P if cluster jobs = 0, pct < 5 %, D, pct > 5%, 1-6 = DataNodes down
	// Temp cold = cluster pct < 5, n > 5 < 70, h > 70
	private static char gear = 'p';
	private static char temp = 'c';
	
	private static String[] tmpString = new String[20];
	private static String[] gngTmpString = new String[20];
	
	private static final int TIME_OUT = 2000;
	private static final int DATA_RATE = 9600;
	
	// Other lights (cruise, AWD, brake, etc) can come later.
	private static String serialPort = "";
	private static HttpURLConnection con;
	@SuppressWarnings("restriction")
	public static void main (String[] args) throws Exception {

		// Create the "trust anything" trust manager.
		// Code "borrowed" from http://stackoverflow.com/questions/13022717/java-and-https-url-connection-without-downloading-certificate
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        } };
		
        // Install the all-trusting trust manager
        final SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        
        // Create all-trusting host name verifier
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        
		Map<String,String> ambariCalls = new HashMap<String,String>();

		ambariCalls.put("namenode", "/services/HDFS/components/NAMENODE");
		ambariCalls.put("resourcemanager", "/services/YARN/components/RESOURCEMANAGER");
		ambariCalls.put("yarn", "/services/YARN");
		ambariCalls.put("hdfs", "/services/HDFS");
		ambariCalls.put("nagios", "/services/AMBARI_METRICS");
		ambariCalls.put("hive", "/services/HIVE");
		ambariCalls.put("mr2", "/services/MAPREDUCE2");
		ambariCalls.put("hbase", "/services/HBASE");
		ambariCalls.put("metricsCPU","?fields=metrics/cpu/Idle._avg");
		
		SerialPort sp;
		String gangliaHost = new String();
		String userName = new String();
		String password = new String();
		String cmdArg = new String();
		String cmdValue = new String();
		String ambariUrl = "";
		String clusterName = new String();
		String authString = new String();
		int sleepTime = 15000;
		
		
		for (String a: args) {
			cmdArg = a.substring(0,a.indexOf(":") );
			cmdValue = a.substring(a.indexOf(":") + 1);
			
			if (cmdArg.equals("-port")) {
				serialPort=cmdValue;
			}
			if (cmdArg.equals("-authString")) {
				authString=cmdValue;
			}
			if (cmdArg.equals("-ambariUrl")) { 
				//if (! ambariUrl.startsWith("https")) {
				//	System.out.println("Only HTTPS is supported.  Exiting.");
				//	System.exit(1);
				//}
				ambariUrl=cmdValue;
			}

			if (cmdArg.equals("-sleepTime")) { 
				sleepTime=Integer.parseInt(cmdValue);
			}
			if (cmdArg.equals("-clusterName")) { 
				clusterName=cmdValue;
			}
			
			if (cmdArg.equals("-bufferSeconds")) { 
				bufferSeconds=Integer.parseInt(cmdValue);
			}
		}
		
		if (serialPort.equals("")) {
			System.out.println("-port argument missing.  Aborted.");
			System.exit(1);
		}
		if (ambariUrl.equals("")) {
			System.out.println("-ambariUrl argument missing.  Aborted.");
			System.exit(1);
		}

		if (clusterName.equals("")) {
			System.out.println("-clusterName argument missing.  Aborted.");
			System.exit(1);
		}
		
		CommPortIdentifier portId = null;
		Enumeration portEnum = CommPortIdentifier.getPortIdentifiers();

		//First, Find an instance of serial port as set in PORT_NAMES.
		while (portEnum.hasMoreElements()) {
			CommPortIdentifier currPortId = (CommPortIdentifier) portEnum.nextElement();

			if (currPortId.getName().equals(serialPort)) {
				portId = currPortId;
				break;
			}

		}
		if (portId == null) {
			System.out.println("Could not find the specified port: " + serialPort);
			return;
		}
		
		try {
			// open serial port, and use class name for the appName.
			sp = (SerialPort) portId.open("AmbariGauges",
					TIME_OUT);

			// set port parameters
			sp.setSerialPortParams(DATA_RATE,
					SerialPort.DATABITS_8,
					SerialPort.STOPBITS_1,
					SerialPort.PARITY_NONE);

			// open the streams
			input = new BufferedReader(new InputStreamReader(sp.getInputStream()));
			output = sp.getOutputStream();

		} catch (Exception e) {
			System.err.println(e.toString());
			System.exit(1);
		}
		
		// If no auth string from the command line.
		if (authString.equals("") || authString.isEmpty()) {
			
			// Ask for username and password for use in Basic Auth
			System.out.println("Enter Ambari username:");
			userName = System.console().readLine().toString();
			System.out.println("Enter Ambari password:");
			
			password = System.console().readPassword().toString();
			byte[] b = Base64.encodeBase64(new String(userName+":"+password).getBytes());
			authString = new String(b);
		}
		//userName="username";
		//password="password";
				
		JsonObject[] jsonObjects = new JsonObject[20];
		while (true) {
		
		// Set up the connection
		try {
		
		  
		  
		  for (int i = 0; i < jsonCalls.length; i++){

			String restCall = ambariCalls.get(jsonCalls[i]);
			//System.out.println(ambariUrl+"/api/v1/clusters/" + clusterName + restCall);

			URL urlObj = null;
			
			if (jsonCalls[i].equals("metricsCPU")) {
				// Ambari Metrics requires timestamp
				Long startTime = (System.currentTimeMillis() / 1000L) - bufferSeconds;
				Long endTime = System.currentTimeMillis() / 1000L;
				//System.out.println(ambariUrl+"/api/v1/clusters/" + clusterName + restCall + "[" + startTime.toString() + "," + endTime.toString() + ",15]");
				urlObj = new URL(ambariUrl+"/api/v1/clusters/" + clusterName + restCall + "[" + startTime.toString() + "," + endTime.toString() + ",15]");				
			} else {
				urlObj = new URL(ambariUrl+"/api/v1/clusters/" + clusterName + restCall);
			}
			

			con = (HttpURLConnection) urlObj.openConnection();	

			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent", USER_AGENT);
			con.setRequestProperty("Authorization", "Basic "+ authString);
			
			
			BufferedReader in = null;
			
			in = new BufferedReader(new InputStreamReader(con.getInputStream()));

			String inputLine;
			StringBuffer response = new StringBuffer();
			
			while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
			}
			
			in.close();	
			
			//System.out.println(response.toString());

			jsonObjects[i] = new Gson().fromJson(response.toString(),JsonObject.class);
			
		  }
		  
		  
		} catch (Exception e) {
			e.printStackTrace();
			//System.out.println(con.getResponseMessage());
			//System.exit(1);
			System.out.println("Failed to get data from either Ganglia or Ambari.  Trying again in " + sleepTime/1000 + " seconds.");
			error = 1;
		}
		
		if (error == 0) {
		// 0 = namenode, 1 = mapreduce/yarn
		// Parse, set variables, resend
		int alerts = 0;
		float tmpVal = 0;
		JsonObject jTmp;
		JsonArray jArray;
		String b = new String();
		
		// ** FIGURE IT OUT!! **
		// Speed (Cluster Pct)
		float tmpFloat = 0;
		try {
			//was 8
			jArray = jsonObjects[7].get("metrics")
					.getAsJsonObject().get("cpu")
					.getAsJsonObject().getAsJsonArray("Idle._avg");
					//System.out.println(jArray.toString());
					//System.out.println(jArray.get(1).getAsJsonArray().get(0).toString());
					int backCount = jArray.size() - 1;
					String cpuValue = "0.0";
			
					while (backCount > 0 && cpuValue.equals("0.0")) {
						cpuValue = jArray.get(backCount).getAsJsonArray().get(0).toString();
						backCount --;
					}
					
					// If no number, either 0% busy or set wrong.  Don't show 100.
					if (cpuValue.equals("0.0")){
						cpuValue = "100";
					}
					
					tmpFloat = 100 - (Float.parseFloat(cpuValue));
					
					//System.out.println(tmpFloat + " " + clusterPct);
					
		}catch (Exception e){
			System.out.println("Failed to get CPU metrics from AMBARI_METRICS  Retrying at next poll.");
			e.printStackTrace();
			tmpFloat = clusterPct;
		}
		
		//System.out.println("Percent Busy: " + (int)tmpFloat);
		if ((int)tmpFloat != clusterPct) {

			clusterPct = (int)tmpFloat;
			// Send the updated Speed to the cluster.
			if (clusterPct >= 10 && clusterPct < 100) {
				b = "S0" + clusterPct + "\n";;
			} else if (clusterPct < 10 ){
				b = "S00" + clusterPct + "\n";
			} else {
				b = "S" + clusterPct + "\n";
			}
			System.out.println("Speed: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
		}
		
		// Tach (# jobs)
		try {
			jTmp = jsonObjects[1].get("metrics")
					.getAsJsonObject().get("yarn")
					.getAsJsonObject().get("Queue")
					.getAsJsonObject().getAsJsonObject("root");
					tmpVal = Float.parseFloat(jTmp.get("AppsRunning").toString());
		}catch (Exception e){
			System.out.println("Failed to get jobs metrics from YARN.  Retrying at next poll.");
			tmpVal = clusterJobs;
		}
		
		if ((int)tmpVal != clusterJobs) {
			clusterJobs = (int) tmpVal;
			// Send the updated Tach message.
	
			if (clusterJobs < 80) {
				if (clusterJobs < 10 ) {
					b = "T00" + clusterJobs + "\nR001\n"; 
				} else {
					b = "T0" + clusterJobs + "\nR002\n";
				}
					
			} else {
				b = "T080\nR002\n";
			}
					

			System.out.println("Tach: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
		}

		// Fuel (HDFS PCT FREE)
		try {
			jTmp = jsonObjects[0].get("metrics")
				.getAsJsonObject().get("dfs")
				.getAsJsonObject().getAsJsonObject("FSNamesystem");

			tmpVal = 100 - ((Float.parseFloat(jTmp.get("CapacityUsedGB").toString()) / Float.parseFloat(jTmp.get("CapacityTotalGB").toString()))*100);
		} catch (Exception e) {
			System.out.println("Error: FS Name System metrics were missing.  Skipping this time.");
			tmpVal = hdfsPct;
		}
		if ((int)tmpVal != hdfsPct) {
			hdfsPct = (int) tmpVal;
			// Send the updated fuel message.
			if (hdfsPct >= 10 && hdfsPct < 100) {
				b = "F0" + hdfsPct + "\n";;
			} else if (hdfsPct < 10 ){
				b = "F00" + hdfsPct + "\n";
			} else {
				b = "F" + hdfsPct + "\n";
			}
			System.out.println("Fuel: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
		}
		
		// Oil (HDFS CRITICAL SERVICES)
		try {
			jTmp = jsonObjects[3].getAsJsonObject("alerts_summary");
			tmpVal = Float.parseFloat(jTmp.get("CRITICAL").toString());
		} catch (Exception e) {
			System.out.println("Failed to get HDFS error status.  Trying at next poll.");
			tmpVal = hdfsAlerts;
		}
		if (hdfsAlerts != (int)tmpVal) {
			hdfsAlerts = (int) tmpVal;
			// Send it!
			if (hdfsAlerts > 0) {
				b = "O001\n";
			} else {
				b = "O000\n";
			}
			System.out.println("Oil: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
			
		}
		
		// Volts (YARN alerts)
		try {
			jTmp = jsonObjects[2].getAsJsonObject("alerts_summary");
			tmpVal = Float.parseFloat(jTmp.get("CRITICAL").toString());
		} catch (Exception e) {
			System.out.println("Failed to get YARN alerts.  Retrying at next poll.");
			tmpVal = yarnAlerts;
		}
		if (yarnAlerts != (int)tmpVal) {
			yarnAlerts = (int) tmpVal;
			// Send it!
			if (yarnAlerts > 0) {
				b = "V001\n";
			} else {
				b = "V000\n";
			}
			System.out.println("Volts: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
			
		}
		
		
		// AT OIL TEMP (Ambari Metrics services down)
		try {
			jTmp = jsonObjects[4].getAsJsonObject("alerts_summary");
			tmpVal = Float.parseFloat(jTmp.get("CRITICAL").toString());
		} catch (Exception e) {
			System.out.println("Failed to get AMBARI_METRICS alerts.  Retrying at next poll.");
			tmpVal = monAlerts;
		}
		if (monAlerts != (int)tmpVal) {
			monAlerts=(int) tmpVal;
			// Send it!
			if (monAlerts > 0) {
				b = "I001\n";
			} else {
				b = "I000\n";
			}
			System.out.println("AT OIL TEMP: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
		}
		alerts=0;
		// Check Engine
		try {
			// WAS 2 to 8, changed to 2 to 7
			for (int o = 2; o < 7; o++) {
				jTmp = jsonObjects[o].getAsJsonObject("alerts_summary");
				tmpVal = Float.parseFloat(jTmp.get("CRITICAL").toString());
				alerts += tmpVal;
			}
		} catch (Exception e) {
			System.out.println("Failed to get cluster-wide alerts for MIL.  Retrying at next poll.");
			alerts = alertCount;
		}
		if (alertCount != alerts){
			alertCount = alerts;
			// Send it!
			if (alertCount == 0) {
				b = "M000\n";
			} else if (alertCount > 0 && alertCount < 10) {
				b = "M001\n";
			} else {
				b = "M002\n";
			}
			System.out.println("Check Engine: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
			
		}
		
		// Gear - Jobs + cluster Percentage - or error.
		char gearTmp;
		if (clusterJobs > 0 && clusterPct > 5) {
			gearTmp='d';
		} else if (clusterJobs == 0 && clusterPct == 0) {
			gearTmp='p';
		} else {
			gearTmp='n';
		}
		if (! (gearTmp == gear)) {
			gear = gearTmp;
			// Send it!
			b="G" + gear + "\n";
			System.out.println("Gear: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
			
		}
		// Temp - Temperature light based on CPU utilization.
		char tempTmp;
		if (clusterPct < 5) {
			tempTmp='c';
		} else {
			if (clusterPct > 4 && clusterPct < 70) {
				tempTmp='n';
			} else {
				tempTmp='h';
			}
		}
		if (!(tempTmp==temp)) {
			temp=tempTmp;
			// Send it!
			b="P" + temp + "\n";
			System.out.println("Temp: " + new String(b.getBytes()));
			output.write(b.getBytes());
			Thread.sleep(200);
		}
		}
		
		// Reset error for the next run.
		error = 0;
		// Sleep and repeat!
		Thread.sleep(sleepTime);
	  }

	}
	
}
