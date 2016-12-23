package org.kisti.htc.scoutmanager;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream.GetField;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.kisti.htc.dbmanager.DbCe;
import org.kisti.htc.dbmanager.DbCe;
import org.kisti.htc.dbmanager.DbCeStatistic;
import org.kisti.htc.dbmanager.DbCeStatistic;
import org.kisti.htc.dbmanager.DbJoin;
import org.kisti.htc.dbmanager.DbJoin;
import org.kisti.htc.dbmanager.DbScout;
import org.kisti.htc.dbmanager.DbScout;
import org.kisti.htc.dbmanager.DbVo;
import org.kisti.htc.dbmanager.DbVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScoutManager {

	private List<CeInfo> ceBeans;
	private DbCe dbCe;
	private DbScout dbScout;
	private DbVo dbVo;
	private DbJoin dbJoin;
	private DbCeStatistic dbCeStatistic;

	// private ServerSocket scoutSocket;
	private Queue<ScoutInfo> scoutQueue;
	// private Queue<ScoutBean> scoutRecordQueue;

	private BlockingQueue<ScoutInfo> scoutRecordBlockingQueue;
	private BlockingQueue<ScoutInfo> scoutBlockingQueue;
	private BlockingQueue<ScoutInfo> scoutResubmissionBlockingQueue;

	private Queue<ScoutInfo> scoutResubmissionQueue;

	private boolean emitterOn;
	private boolean serverOn;
	private boolean scoutRecorderOn;

	// private boolean serverError;
	private ScoutEmitter emitterUtils;
	private List<EmitterPrimary> emitterPrimaries;
	private EmitterSecondary[] emitterSecondaries;
	private ScoutCollector[] scoutServers;

	private ScoutRecorder[] scoutRecorders;

	private Map<Integer, String> ceIdVoName = new HashMap<Integer, String>();

	private static final Logger logger = LoggerFactory
			.getLogger(ScoutManager.class);

	public ScoutManager() {

		setDbCe(new DbCe());
		setDbScout(new DbScout());
		setVoDAO(new DbVo());
		setJoinDAO(new DbJoin());
		setCeStatisticDAO(new DbCeStatistic());
		setScoutQueue(new LinkedList<ScoutInfo>());
		setScoutResubmissionQueue(new LinkedList<ScoutInfo>());
		setEmitterUtils(new ScoutEmitter());
		initVomsProxies();
		setCeList(getDbCe().collectCes());// Get list of Ces on databases
		getDbScout().deleteAllScoutsInScoutTable();

		for (CeInfo ceBean : getCeList()) {
			cancelAllScoutsOnCe(ceBean);
			addScoutsToQueue(ceBean, 0);// put request to the Queue
		}

		startScoutCollectors();
		try {
			Thread.sleep(6000);
		} catch (Exception e) {
			// TODO: handle exception
		}

		startScoutEmitters();
		try {
			Thread.sleep(6000);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	private void initVomsProxies() {

		List<VoInfo> vos = getVoDAO().collectVos();

		for (VoInfo vo : vos) {
			File proxyFile = new File(Constants.gliteDir, vo.getName()
					+ ".proxy");
			String command = "voms-proxy-init -cert "
					+ Constants.gliteDir.getPath() + "/usercert.pem -key "
					+ Constants.gliteDir.getPath() + "/userkey.pem " + "-out "
					+ proxyFile.getPath() + " -voms " + vo.getName()
					+ " -pwstdin";
			try {
				System.out.println("command = " + command);
				Process p = Runtime.getRuntime().exec(command);
				DebugLog.log(command);
				PrintStream fout = new PrintStream(p.getOutputStream());
				BufferedReader fr = new BufferedReader(new FileReader(
						Constants.gliteDir.getPath() + "/.gridproxy"));
				String line;
				while ((line = fr.readLine()) != null) {
					fout.println(line);
				}
				fout.flush();
				int exitValue = p.waitFor();
				System.out.println("exitValue = "+ String.valueOf(exitValue));
				if (exitValue == 0) {
					DebugLog.log("Successfully init voms proxy for VO: "
							+ vo.getName());
					for (int i = 0; i < 10; i++) {
						command = "fuser -k " + String.valueOf(i + 9100)
								+ "/tcp";
						DebugLog.log("Kill port:" + command);
						Runtime.getRuntime().exec(command);
					}

				} else {
					DebugLog.log("Fail init voms proxy for VO:" + vo.getName());

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		

	}

	public int getProxyTimeLeft(VoInfo vo) {

		int timeLeft = 0;

		File proxyFile = new File(Constants.gliteDir, vo.getName() + ".proxy");
		if (!proxyFile.exists()) {
			return 0;
		}

		String command = "voms-proxy-info -file " + proxyFile.getPath()
				+ " -timeleft";
		try {
			Process p = Runtime.getRuntime().exec(command);
			DebugLog.log(command);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			String line = br.readLine();
			timeLeft = Integer.parseInt(line);
			br.close();

			StringBuffer sb = new StringBuffer();
			br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while ((line = br.readLine()) != null) {
				sb.append(line + "\n");
			}
			if (sb.toString().contains("Couldn't find a valid proxy")) {
				return 0;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return timeLeft;

	}

	private void startScoutEmitters() {
		DebugLog.log("Start Emitter");
		setEmitterOn(true);
		EmitterController emitterController = new EmitterController();
		emitterController.start();
		DebugLog.logThread();
	}

	private void stopEmitter() {
		setEmitterOn(false);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void startScoutCollectors() {
		DebugLog.log("Start Server");
		setServerOn(true);
		// setServerError(false);
		ServerController serverController = new ServerController();
		serverController.start();
		DebugLog.logThread();
	}

	private void stopServerController() {
		setServerOn(false);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void startScoutRecorders() {
		setScoutRecorderOn(true);
		setScoutRecorders(new ScoutRecorder[Constants.numScoutRecorder]);
		for (int i = 0; i < getScoutRecorders().length; i++) {
			// DebugLog.log(i);
			scoutRecorders[i] = new ScoutRecorder("" + i);
			scoutRecorders[i].start();
			DebugLog.logThread();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void stopScoutRecorders() {
		setScoutRecorderOn(false);
	}

	private void stopAll() {
		stopEmitter();
		stopServerController();
		// stopScoutRecorders();
	}

	private ScoutInfo extractScoutInfo(String reportString) {

		/*
		 * Report from client like scoutNo: 100, ceId: 0, wn: 150.183.250.128,
		 * submitTime: 1405430215747, arrivalTime: 1406126858015,
		 * lastResponseTime: 1406126858023
		 */

		// DebugLog.log(reportString);

		ScoutInfo scoutBean = null;
		String[] parts = reportString.split(" ");

		int id = new Integer(parts[1].substring(0, parts[1].length() - 1));
		int ceId = new Integer(parts[3].substring(0, parts[3].length() - 1));
		String wn = parts[5].substring(0, parts[5].length() - 1);
		long submitTimeMillis = new Long(parts[7].substring(0,
				parts[7].length() - 1));
		long arrivalTime = new Long(
				parts[9].substring(0, parts[9].length() - 1));
		long lastReportTime = new Long(parts[11]);

		for (CeInfo ceBean : getCeList()) {

			if (ceBean.getId() == ceId) {
				DebugLog.logMain("reset time and submit time = "
						+ ceBean.getResetTime() + submitTimeMillis);
				if (ceBean.getResetTime() < submitTimeMillis) {

					if (ceBean.getScoutFirstArivalTime() == 0) {

						ceBean.setFirstScoutArrivalTime(arrivalTime);
					}
					//code for scout report***********
					/*if ((arrivalTime - ceBean.getScoutFirstArivalTime()) < Constants.estimatedClusterJobAllocatingTime) {
						scoutBean = new ScoutInfo(id, ceId);
						scoutBean.setIpAddress(wn);
						scoutBean.setArrivalTime(arrivalTime);
						scoutBean.setLastResponseTime(lastReportTime);
						break;
					} else {
						System.out.println("It arrivaled too late");
					}*/
					//new 
					scoutBean = new ScoutInfo(id, ceId);
					scoutBean.setIpAddress(wn);
					scoutBean.setArrivalTime(arrivalTime);
					scoutBean.setLastResponseTime(lastReportTime);

				}

			}

		}
		return scoutBean;
	}

	private void removeUnsubmittedScoutsInQueue(int ceId) {
		Iterator it1 = getScoutQueue().iterator();
		// Iterator it1 = getScoutBlockingQueue().iterator();

		while (it1.hasNext()) {
			ScoutInfo scoutBean = (ScoutInfo) it1.next();
			if ((scoutBean != null) && (scoutBean.getCeId() == ceId)) {
				it1.remove();
			}
		}

		Iterator it2 = getScoutResubmissionQueue().iterator();
		// Iterator it2 = getScoutResubmissionBlockingQueue().iterator();
		while (it2.hasNext()) {
			ScoutInfo scoutBean = (ScoutInfo) it2.next();
			if ((scoutBean != null) && (scoutBean.getCeId() == ceId)) {
				it2.remove();
			}
		}
	}

	private void cancelAllScoutsOnCe(CeInfo ceBean) {

		String[] parts = ceBean.getName().split("/");
		// String voName = getJoinDAO().findVoForCe(ceBean).getName();
		String voName = "biomed";

		List<String> comm1 = new ArrayList<String>();
		comm1.add("glite-ce-job-cancel");
		comm1.add("--noint");
		comm1.add("-e");
		comm1.add(parts[0]);
		comm1.add("-a");

		List<String> comm2 = new ArrayList<String>();
		comm2.add("glite-ce-job-purge");
		comm2.add("--noint");
		comm2.add("-e");
		comm2.add(parts[0]);
		comm2.add("-a");

		File proxyFile = new File(Constants.gliteDir, voName + ".proxy");

		ProcessBuilder builder1 = new ProcessBuilder(comm1);
		DebugLog.log(comm1);
		Map<String, String> envs1 = builder1.environment();
		envs1.put("X509_USER_PROXY", proxyFile.getAbsolutePath());

		// ProcessBuilder builder2 = new ProcessBuilder(comm1);
		ProcessBuilder builder2 = new ProcessBuilder(comm2);// fix bug
		DebugLog.log(comm2);
		Map<String, String> envs2 = builder2.environment();
		envs2.put("X509_USER_PROXY", proxyFile.getAbsolutePath());

		try {
			builder1.start();
			builder2.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// scoutDAO.cleanAllScountsOnCe(ceBean);
	}

	//
	private void addScoutsToQueue(CeInfo ceBean, int round) {

		removeUnsubmittedScoutsInQueue(ceBean.getId());// remove the same ce in
														// the
														// queue

		getDbScout().deleteScoutInScoutTable(ceBean);
		DebugLog.logScout("------");

		int numScout = Constants.numScoutPerCePerRound.get(round);
		int currentMaxScoutId = getDbScout().findMaxScoutIdFromScoutTable();
		DebugLog.logScout("------");
		currentMaxScoutId++;
		for (int i = 0; i < numScout; i++) {

			ScoutInfo scoutBean = new ScoutInfo(currentMaxScoutId,
					ceBean.getId());

			getScoutQueue().add(scoutBean);

			dbScout.insertScout(scoutBean);
			DebugLog.logScout("------");

			currentMaxScoutId++;
		}

		initCeInfo(ceBean);
		ceBean.setAtRound(round);
		ceBean.setAllowMoreScouts(false);// reset

		DbCeStatistic ceStatistic = new DbCeStatistic();
		ceStatistic.addScoutInfotoCeStatistic(ceBean);
		DebugLog.logCestatistic("-------------insert---------");

	}

	private void initCeInfo(CeInfo ceBean) {

		ceBean.setResetTime(System.currentTimeMillis());
		ceBean.setNumSuccessSubmittedScout(0);
		ceBean.setNumArrivedScout(0);
		ceBean.setFirstScoutArrivalTime(0);
		ceBean.setAvgResponseTime(0);

	}

	private String convertLongToDate(long dateMillis) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
				Locale.US);

		GregorianCalendar calendar = new GregorianCalendar(
				TimeZone.getTimeZone("US/Central"));
		calendar.setTimeInMillis(dateMillis);
		return sdf.format(calendar.getTime());
	}

	public static void main(String[] args) {

		DebugLog.log("Starting time: " + System.currentTimeMillis());

		long startTime = System.currentTimeMillis();
		DebugLog.log("Main started");
		int condition = 1;
		while (condition > 0) {

			DebugLog.logMain("Creat new Scout Manager");

			ScoutManager scoutManager = new ScoutManager();

			DebugLog.logMain("Created successfully");

			DebugLog.logMain("coutManager : " + scoutManager.toString());

			for (int i = 0; i < Constants.numScoutPerCePerRound.size(); i++) {
				// Wait until all scouts submitted
				while (scoutManager.getScoutQueue().size() != 0) {
					DebugLog.logMain("Queue size is = "
							+ scoutManager.getScoutQueue().size());
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				try {
					Thread.sleep(Constants.estimatedClusterJobAllocatingTime);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				DebugLog.logMain("Start evaluationg CE at Round = " + i);

				for (CeInfo ceBean : scoutManager.getCeList()) {

					long oldResetTime = ceBean.getResetTime();
					scoutManager.getDbScout().getScoutInfoFromDb(ceBean);

					scoutManager.getDbCeStatistic().updateScoutInfoToDb(ceBean,
							oldResetTime);

					if (ceBean.getNumArrivedScout() != 0) {
						scoutManager.getDbCe().updateCeInfoToDb(ceBean);
					}

					long scoutStayingTime = System.currentTimeMillis()
							- ceBean.getScoutFirstArivalTime();

					if ((ceBean.getNumArrivedScout() > 0)&&(ceBean.getAtRound() < Constants.numScoutPerCePerRound
							.size() - 1)) {

						if (((double) ceBean.getNumArrivedScout())
								/ ((double) Constants.numScoutPerCePerRound
										.get(ceBean.getAtRound())) > Constants.successRateThreshold) {

							scoutManager.removeUnsubmittedScoutsInQueue(ceBean
									.getId());
							scoutManager.cancelAllScoutsOnCe(ceBean);
							scoutManager.getDbScout().deleteScoutInScoutTable(
									ceBean);
							DebugLog.logScout("------");
							ceBean.setAllowMoreScouts(true);

						} else if ((scoutStayingTime > Constants.estimatedClusterJobAllocatingTime)
								&& (scoutStayingTime < 2*Constants.estimatedClusterJobAllocatingTime)) {
							DebugLog.logMain();
							scoutManager.cancelAllScoutsOnCe(ceBean);
							scoutManager.initCeInfo(ceBean);
						}
					}

				}

				for (CeInfo ceBean : scoutManager.getCeList()) {
					// DebugLog.logMain();
					if (ceBean.isAllowMoreScouts()
							&& (ceBean.getAtRound() < Constants.numScoutPerCePerRound
									.size() - 1)) {
						int currentRoundOfCe = ceBean.getAtRound();
						int nextRoundOfCe = currentRoundOfCe + 1;
						ceBean.setAtRound(nextRoundOfCe);
						scoutManager.addScoutsToQueue(ceBean, nextRoundOfCe);
						DebugLog.logScout("------");
					}
				}

			}
			if (scoutManager.getScoutQueue().size() > 0) {
				while (scoutManager.getScoutQueue().size() != 0) {
					DebugLog.logMain("Queue size is = "
							+ scoutManager.getScoutQueue().size());
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				try {
					Thread.sleep(Constants.estimatedClusterJobAllocatingTime);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				DebugLog.logMain("Start evaluationg CE at Final Round");
			}

			scoutManager.stopAll();

			for (CeInfo ceBean : scoutManager.getCeList()) {
				long oldResetTime = ceBean.getResetTime();
				scoutManager.getDbScout().getScoutInfoFromDb(ceBean);
				scoutManager.getDbCeStatistic().updateScoutInfoToDb(ceBean,
						oldResetTime);
			}
			long endTime = System.currentTimeMillis();
			DebugLog.log("Current time:" + endTime);
			long scanTime = (endTime - startTime) / 60000;
			DebugLog.logMain("Scanning Time: " + scanTime + " minutes");
			DebugLog.logMain("Program pause now, all Emiiters and Collecters have stopped");
			// End
			condition--;
			if (condition > 0) {
				try {
					Thread.sleep(Constants.estimatedClusterJobAllocatingTime);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		DebugLog.logMain("Program top now, all Emiiters and Collecters have stopped");
	}

	private List<CeInfo> getCeList() {
		return ceBeans;
	}

	private void setCeList(List<CeInfo> ceBeans) {
		this.ceBeans = ceBeans;
	}

	public DbCe getDbCe() {
		return dbCe;
	}

	public void setDbCe(DbCe ceDAO) {
		this.dbCe = ceDAO;
	}

	public DbScout getDbScout() {
		return dbScout;
	}

	public void setDbScout(DbScout scoutDAO) {
		this.dbScout = scoutDAO;
	}

	public DbVo getVoDAO() {
		return dbVo;
	}

	public void setVoDAO(DbVo voDAO) {
		this.dbVo = voDAO;
	}

	public DbJoin getJoinDAO() {
		return dbJoin;
	}

	public void setJoinDAO(DbJoin joinDAO) {
		this.dbJoin = joinDAO;
	}

	private DbCeStatistic getDbCeStatistic() {
		return dbCeStatistic;
	}

	private void setCeStatisticDAO(DbCeStatistic ceStatisticDAO) {
		this.dbCeStatistic = ceStatisticDAO;
	}

	public boolean isEmitterOn() {
		return emitterOn;
	}

	public void setEmitterOn(boolean emitterOn) {
		this.emitterOn = emitterOn;
	}

	public Queue<ScoutInfo> getScoutQueue() {
		return scoutQueue;
	}

	public void setScoutQueue(Queue<ScoutInfo> scoutQueue) {
		this.scoutQueue = scoutQueue;
	}

	public Queue<ScoutInfo> getScoutResubmissionQueue() {
		return scoutResubmissionQueue;
	}

	public void setScoutResubmissionQueue(
			Queue<ScoutInfo> scoutResubmissionQueue) {
		this.scoutResubmissionQueue = scoutResubmissionQueue;
	}

	public Map<Integer, String> getCeIdVoName() {
		return ceIdVoName;
	}

	public void setCeIdVoName(Map<Integer, String> ceIdVoName) {
		this.ceIdVoName = ceIdVoName;
	}

	public ScoutEmitter getEmitterUtils() {
		return emitterUtils;
	}

	public void setEmitterUtils(ScoutEmitter emitterUtils) {
		this.emitterUtils = emitterUtils;
	}

	public List<EmitterPrimary> getEmitterPrimariesList() {
		return emitterPrimaries;
	}

	public void setEmitterPrimariesList(List<EmitterPrimary> emitterPrimaries) {
		this.emitterPrimaries = emitterPrimaries;
	}

	public EmitterSecondary[] getEmitterSecondaries() {
		return emitterSecondaries;
	}

	public void setEmitterSecondaries(EmitterSecondary[] emitterSecondaries) {
		this.emitterSecondaries = emitterSecondaries;
	}

	public boolean isServerOn() {
		return serverOn;
	}

	public void setServerOn(boolean serverOn) {
		this.serverOn = serverOn;
	}

	// public void setCeBeans(List<CeBean> ceBeans) {
	// this.ceBeans = ceBeans;
	// }
	//

	public BlockingQueue<ScoutInfo> getScoutBlockingQueue() {
		return scoutBlockingQueue;
	}

	public void setScoutBlockingQueue(
			BlockingQueue<ScoutInfo> scoutBlockingQueue) {
		this.scoutBlockingQueue = scoutBlockingQueue;
	}

	public BlockingQueue<ScoutInfo> getScoutResubmissionBlockingQueue() {
		return scoutResubmissionBlockingQueue;
	}

	public void setScoutResubmissionBlockingQueue(
			BlockingQueue<ScoutInfo> scoutResubmissionBlockingQueue) {
		this.scoutResubmissionBlockingQueue = scoutResubmissionBlockingQueue;
	}

	// public boolean isServerError() {
	// return serverError;
	// }
	//
	// public void setServerError(boolean serverError) {
	// this.serverError = serverError;
	// }

	// public Queue<ScoutBean> getScoutRecordQueue() {
	// return scoutRecordQueue;
	// }
	//
	// public void setScoutRecordQueue(Queue<ScoutBean> scoutRecordQueue) {
	// this.scoutRecordQueue = scoutRecordQueue;
	// }

	public boolean isScoutRecorderOn() {
		return scoutRecorderOn;
	}

	public void setScoutRecorderOn(boolean scoutRecorderOn) {
		this.scoutRecorderOn = scoutRecorderOn;
	}

	public ScoutRecorder[] getScoutRecorders() {
		return scoutRecorders;
	}

	public void setScoutRecorders(ScoutRecorder[] scoutRecorders) {
		this.scoutRecorders = scoutRecorders;
	}

	public BlockingQueue<ScoutInfo> getScoutRecordBlockingQueue() {
		return scoutRecordBlockingQueue;
	}

	public void setScoutRecordBlockingQueue(
			BlockingQueue<ScoutInfo> scoutRecordBlockingQueue) {
		this.scoutRecordBlockingQueue = scoutRecordBlockingQueue;
	}

	public ScoutCollector[] getScoutServers() {
		return scoutServers;
	}

	public void setScoutServers(ScoutCollector[] scoutServers) {
		this.scoutServers = scoutServers;
	}

	private class ServerController extends Thread {
		private void startServer(int i) {
			// setServerOn(true);
			DebugLog.log("Starting server socket: port  = "
					+ Integer.toString(9100 + i));
			// setServerError(false);
			getScoutServers()[i] = new ScoutCollector("" + i, 9100 + i);
			getScoutServers()[i].start();
			DebugLog.logThread();
		}

		private void stopServer(int i) {
			// setServerOn(false);
			if (getScoutServers()[i].getScoutSocket() != null) {
				// DebugLog.log(getScoutSocket());
				try {
					DebugLog.logMain("Server will stop now = " + i);
					getScoutServers()[i].getScoutSocket().close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					// DebugLog.log("hohoho");
				}
				// DebugLog.log(getScoutSocket().isClosed());
				// setStopSocket(true);
				// System.exit(0);
			}

		}

		@Override
		public void run() {
			DebugLog.logThread("Start");
			// TODO Auto-generated method stub
			setScoutServers(new ScoutCollector[Constants.numScoutServer]);

			for (int i = 0; i < getScoutServers().length; i++) {
				startServer(i);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			while (isServerOn()) {
				for (int i = 0; i < getScoutServers().length; i++) {
					// DebugLog.log(i);
					if (getScoutServers()[i].isServerError()) {
						DebugLog.log("Found server error");
						stopServer(i);
						startServer(i);
					}
				}
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (int i = 0; i < getScoutServers().length; i++) {
				stopServer(i);
			}
		}
	}

	private class ScoutCollector extends Thread {
		private String name;
		private ServerSocket scoutSocket;
		private boolean serverError;

		public ServerSocket getScoutSocket() {
			return scoutSocket;
		}

		public ScoutCollector(String name, int port) {
			// TODO Auto-generated constructor stub
			this.name = name;
			try {
				this.scoutSocket = new ServerSocket(port);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				DebugLog.log("Server port: " + port);
				e.printStackTrace();
			}
			setServerError(false);
		}

		@Override
		public void run() {
			DebugLog.logThread("Start");
			// TODO Auto-generated method stub
			// DebugLog.log("Stating server in a new thread");
			String clientSentence;
			try {

				while (!getScoutSocket().isClosed() && isServerOn()
						&& !isServerError()) {
					// DebugLog.log("1: " + isStopSocket());

					try {
						Socket connectionSocket = scoutSocket.accept();

						DataInputStream inFromClient = new DataInputStream(
								connectionSocket.getInputStream());
						clientSentence = inFromClient.readUTF();
						DebugLog.logFromScout(clientSentence);
						logger.info(clientSentence);
						System.out.println(clientSentence);
						ScoutInfo scoutBean = extractScoutInfo(clientSentence);
						DataOutputStream outToClient = new DataOutputStream(
								connectionSocket.getOutputStream());
						if (scoutBean != null) {
							dbScout.updateScoutInfoToDb(scoutBean);
							int ceId = scoutBean.getCeId();
							for (CeInfo ceBean : getCeList()) {
								if (ceBean.getId() == ceId) {
									outToClient.writeUTF((new Long(ceBean
											.getScoutFirstArivalTime()))
											.toString());
									break;
								}
							}
						} else {
							outToClient
									.writeUTF((new Long(
											System.currentTimeMillis()
													- Constants.estimatedClusterJobAllocatingTime)).toString());
						}

						/*
						 * if (scoutBean == null) { outToClient.writeUTF((new
						 * Long(System .currentTimeMillis() - 30 * 60000))
						 * .toString()); } else {
						 * dbScout.updateScoutInfoToDb(scoutBean);
						 * DebugLog.logScout("------");
						 * 
						 * int ceId = scoutBean.getCeId(); for (CeInfo ceBean :
						 * getCeList()) { if (ceBean.getId() == ceId) {
						 * outToClient.writeUTF((new Long(ceBean
						 * .getScoutFirstArivalTime())) .toString()); } } }
						 */
						inFromClient.close();
						connectionSocket.close();

					} catch (SocketException e) {
						setServerError(true);

						getScoutSocket().close();
						break;

					} catch (EOFException e) {
						logger.info("EOFException");
						setServerError(true);
						logger.info("Server Error: " + isServerError());
						getScoutSocket().close();
						break;
					}
				}
				DebugLog.logMain("ScoutCollector  " + name + " is Closed");
				getScoutSocket().close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}
			// System.exit(0);
		}

		public boolean isServerError() {
			return serverError;
		}

		public void setServerError(boolean serverError) {
			this.serverError = serverError;
		}

	}

	private class ScoutRecorder extends Thread {
		private String name;

		public ScoutRecorder(String name) {
			// TODO Auto-generated constructor stub
			this.name = name;
		}

		@Override
		public void run() {
			DebugLog.logThread("Start");
			// TODO Auto-generated method stub
			while (isScoutRecorderOn()) {
				// ScoutBean scoutBean = getScoutRecordQueue().poll();

				ScoutInfo scoutBean = null;
				try {
					scoutBean = getScoutBlockingQueue().take();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				if (scoutBean != null) {
					getDbScout().updateScoutInfoToDb(scoutBean);
					DebugLog.logScout("------");
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private class EmitterController extends Thread {

		@Override
		public void run() {
			DebugLog.log("Start EmitterController");
			DebugLog.logThread("Start");
			// TODO Auto-generated method stub
			setEmitterPrimariesList(new ArrayList<EmitterPrimary>());

			while (getEmitterPrimariesList().size() < Constants.numEmitterPrimary) {

				EmitterPrimary emitterPrimary = new EmitterPrimary(""
						+ System.currentTimeMillis());
				getEmitterPrimariesList().add(emitterPrimary);
				emitterPrimary.start();
				DebugLog.logThread();
			}

			setEmitterSecondaries(new EmitterSecondary[Constants.numEmitterSecondary]);

			DebugLog.logThread("Secondaries length = "
					+ String.valueOf(emitterSecondaries.length));

			for (int i = 0; i < emitterSecondaries.length; i++) {

				emitterSecondaries[i] = new EmitterSecondary("" + i);
				emitterSecondaries[i].start();
				DebugLog.logThread();
			}

			while (isEmitterOn()) {
				if (getEmitterPrimariesList().size() < Constants.numEmitterPrimary) {

					EmitterPrimary emitterPrimary = new EmitterPrimary(""
							+ System.currentTimeMillis());
					getEmitterPrimariesList().add(emitterPrimary);
					emitterPrimary.start();
					DebugLog.logThread();
				}
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			DebugLog.log("Stop emitter controller");
		}
	}

	private class EmitterPrimary extends Thread {

		private String emitterName;

		private String getEmitterName() {
			return emitterName;
		}

		private void setEmitterName(String emitterName) {
			this.emitterName = emitterName;
		}

		public EmitterPrimary(String name) {
			// TODO Auto-generated constructor stub
			setEmitterName(name);
		}

		@Override
		public void run() {
			DebugLog.logThread("Start");

			String jobId = "";
			long startTime;
			boolean singleSubmissionError = false;
			ScoutInfo tmpScoutBean = null;
			while (getScoutQueue().size() == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			ScoutInfo scoutBean = getScoutQueue().poll();
			tmpScoutBean = scoutBean;
			while (isEmitterOn() && !singleSubmissionError) {
				if (scoutBean != null) {
					startTime = System.currentTimeMillis();
					jobId = getEmitterUtils().submitSingleScout(scoutBean);

					DebugLog.log("Submitting time for one Scout is :"
							+ (System.currentTimeMillis() - startTime)
							+ "(miliseconds)");

					if (jobId == null) {
						singleSubmissionError = true;
						jobId = "Error!!!!!!!!!!NULL";
						for (CeInfo ceBean : getCeList()) {
							if ((ceBean.getId() == scoutBean.getCeId())
									&& (ceBean.getAtRound() == 0)) {
								singleSubmissionError = false;
								break;
							}
						}
					} else if (jobId.toLowerCase().contains("error")
							|| jobId.toLowerCase().contains("fatal")
							|| jobId.toLowerCase().contains("failed")) {
						singleSubmissionError = true;
						for (String str : Constants.ceErrorPatterns) {
							if (jobId.toLowerCase().contains(str.toLowerCase())) {
								for (CeInfo ceBean : getCeList()) {
									if ((ceBean.getId() == scoutBean.getCeId())
											&& (ceBean.getAtRound() == 0)) {
										singleSubmissionError = false;
										break;
									}
								}
							}
						}
					}
					DebugLog.log("-----------JobId = " + jobId);
				}
				while (getScoutQueue().size() == 0) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				scoutBean = getScoutQueue().poll();
				tmpScoutBean = scoutBean;
			}
			if (tmpScoutBean != null) {
				for (CeInfo ceBean : getCeList()) {
					if ((ceBean.getId() == tmpScoutBean.getCeId())
							&& (ceBean.getAtRound() != 0)) {
						getScoutResubmissionQueue().add(tmpScoutBean);
					}
				}
			}

			Iterator<EmitterPrimary> iterator = getEmitterPrimariesList()
					.iterator();
			while (iterator.hasNext()) {
				if (iterator.next().getEmitterName().equals(getEmitterName())) {
					iterator.remove();
					DebugLog.log("-------------iterator.remove()-----------------");
				}
			}
			DebugLog.logMain("EmitterPrimary  " + emitterName + " is Closed");
		}
	}

	private class EmitterSecondary extends Thread {

		private String name;

		public String getEmitterName() {
			return name;
		}

		public EmitterSecondary(String name) {
			// TODO Auto-generated constructor stub
			this.name = name;
		}

		@Override
		public void run() {
			DebugLog.logThread("Start");
			// TODO Auto-generated method stub

			while (isEmitterOn()) {
				while (getScoutResubmissionQueue().size() == 0) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				ScoutInfo scoutBean = getScoutResubmissionQueue().poll();
				if (scoutBean != null) {
					DebugLog.log("Resubmiting scout " + scoutBean.getId()
							+ " to ce " + scoutBean.getCeId());
					getEmitterUtils().submitSingleScout(scoutBean);
					DebugLog.logScout("------");
				}
				try {
					Thread.sleep(1000);
					DebugLog.logThread(getEmitterName());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			DebugLog.logMain("EmitterSecondary  " + name + " is Closed");
		}
	}

}
