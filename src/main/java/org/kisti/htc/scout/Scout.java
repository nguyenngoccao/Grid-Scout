package org.kisti.htc.scout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.ScoutInfo;

public class Scout {

	private String server = "150.183.250.128";
	private int numTry;
	private boolean foundServer;

	public Scout() {
		// TODO Auto-generated constructor stub
		setNumTry(0);
		setFoundServer(false);
	}

	private long submitInfoToServer(ScoutInfo scoutBean) {
		setNumTry(0);
		setFoundServer(false);

		long fisrtColleagueArrivalTime = 0;
		while (getNumTry() < 100 && !isFoundServer()) {
			fisrtColleagueArrivalTime = responseToTheServer(scoutBean);
		}
		return fisrtColleagueArrivalTime;
	}

	private long responseToTheServer(ScoutInfo scoutBean) {
		int tmpNumTry = getNumTry();
		System.out.println("tmpNumTry: " + tmpNumTry);
		setNumTry(tmpNumTry + 1);

		double random = Math.random();
		int port = (int) Math.round(random * 10) + 9100;

		String message = scoutBean.toString();
		System.out.println(message);
		System.out.println("Port: " + port);
		long firstColleagueArrivalTime = 0;
		try {
			Socket clientSocket = new Socket(server, port);

			DataOutputStream outToServer = new DataOutputStream(
					clientSocket.getOutputStream());
			outToServer.writeUTF(message);

			DataInputStream in = new DataInputStream(
					clientSocket.getInputStream());
			// System.out.println("Client: " + in.readUTF());
			firstColleagueArrivalTime = new Long(in.readUTF());
			outToServer.close();
			in.close();
			clientSocket.close();

			// System.out.println(firstCollegueArrivalTime);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (firstColleagueArrivalTime != 0) {
			setFoundServer(true);
		}

		// System.out.println(firstColleagueArrivalTime);
		return firstColleagueArrivalTime;
	}

	public static void main(String[] args) {

		long arrivalTime = System.currentTimeMillis();

		int id = new Integer(args[0]);
		long submittingTime = new Long(args[1]);
		int ceId = new Integer(args[2]);

/*		int id = 1;
		long submittingTime = arrivalTime;
		int ceId = 12345;*/

		ScoutInfo scoutInfo = new ScoutInfo(id, ceId);
		scoutInfo.setSubmittingTime(submittingTime);

		scoutInfo.setArrivalTime(arrivalTime);
		scoutInfo.setLastResponseTime(0);

		String ipAddress;

		try {
			ipAddress = InetAddress.getLocalHost().getHostAddress();
			scoutInfo.setIpAddress(ipAddress);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Scout scout = new Scout();

		int count = 0;

		while (count < 2) { // run job in 10 mins, report every 5 min
			try {

				long responseTime = System.currentTimeMillis();
				scoutInfo.setLastResponseTime(responseTime);
				long firstColleagueArrivalTime = scout
						.submitInfoToServer(scoutInfo);

				System.out.println("FCAT: " + firstColleagueArrivalTime);

				if ((arrivalTime - firstColleagueArrivalTime) > Constants.estimatedClusterJobAllocatingTime) {
					System.out.println("too late");
					System.exit(0);
				} else {
					count++;
					Thread.sleep(60000 + Constants.estimatedClusterJobAllocatingTime/2);					
				}

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}

	public int getNumTry() {
		return numTry;
	}

	public void setNumTry(int numTry) {
		this.numTry = numTry;
	}

	public boolean isFoundServer() {
		return foundServer;
	}

	public void setFoundServer(boolean foundServer) {
		this.foundServer = foundServer;
	}
}
