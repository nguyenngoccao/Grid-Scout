package org.kisti.htc.scoutmanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kisti.htc.dbmanager.DbCe;
import org.kisti.htc.dbmanager.DbCe;
import org.kisti.htc.dbmanager.DbJoin;
import org.kisti.htc.dbmanager.DbJoin;
import org.kisti.htc.dbmanager.DbScout;
import org.kisti.htc.dbmanager.DbScout;

public class ScoutEmitter {

	private DbScout dbScouts;
	private DbJoin dbJoin;
	private DbCe dbCes;
	// private List<EmitterThread> emitterThreads;
	private List<CeInfo> ces;
	private Map<Integer, String> ceIdVoName = new HashMap<Integer, String>();
	private Map<Integer, String> ceIdCeName = new HashMap<Integer, String>();

	// private QueueUtils queueUtils;

	// private List<EmitterThread> getEmitterThreads() {
	// return emitterThreads;
	// }
	//
	// private void setEmitterThreads(List<EmitterThread> emitterThreads) {
	// this.emitterThreads = emitterThreads;
	// }

	public ScoutEmitter() {
		// TODO Auto-generated constructor stub
		setDbScouts(new DbScout());
		setDbJoin(new DbJoin());
		setCeDAO(new DbCe());
		// setEmitterThreads(new ArrayList<EmitterUtils.EmitterThread>());

		setCes(getCeDAO().collectCes());DebugLog.logSql("------------------");
		// setQueueUtils(new QueueUtils());

		// startEmitters();
	}

	private String findVoForScout(ScoutInfo scoutBean) {

		Integer ceId = scoutBean.getCeId();
		if (getCeIdVoName().containsKey(ceId)) {
			return getCeIdVoName().get(ceId);
		} else {
			// DebugLog.log("will now query database");
			VoInfo vo = getDbJoin().findVoForScout(scoutBean);
			getCeIdVoName().put(scoutBean.getCeId(), vo.getName());
			return vo.getName();
		}
	}

	private String findCeNameForScout(ScoutInfo scoutBean) {
		Integer ceId = scoutBean.getCeId();
		if (getCeIdCeName().containsKey(ceId)) {
			return (getCeIdCeName().get(ceId));
		} else {
			String ceName = getDbJoin().findCeNameForScout(scoutBean);
			getCeIdCeName().put(ceId, ceName);
			return ceName;
		}
	}

	public String submitSingleScout(ScoutInfo scoutBean) {

		String jobId = null;
		// if (!getScoutDAO().isScoutSubmitted(scoutBean)){
		File scoutJDL = generateScoutJDL(scoutBean);

		//String voName = findVoForScout(scoutBean);
		String voName = "biomed";
		String ceName = findCeNameForScout(scoutBean);
		File proxyFile = new File(Constants.gliteDir, voName + ".proxy");
		// DebugLog.log(proxyFile.getAbsolutePath());

		// submit job
		List<String> command = new ArrayList<String>();
		command.add("glite-ce-job-submit");
		command.add("-a");
		command.add("-r");
		command.add(ceName);
		command.add(scoutJDL.getAbsolutePath());

		ProcessBuilder builder = new ProcessBuilder(command);
		DebugLog.log(command);
		Map<String, String> envs = builder.environment();
		envs.put("X509_USER_PROXY", proxyFile.getAbsolutePath());
		builder.directory(Constants.scriptDir);
		String line;
		// String jobId = null;
		Process p;
		try {
			p = builder.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			while ((line = br.readLine()) != null) {
				// DebugLog.log(line);
				jobId = line;
			}
			// DebugLog.log(jobId);
			p.getInputStream().close();
			p.getOutputStream().close();
			p.getErrorStream().close();
			p.waitFor();
			p.destroy();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if ((jobId != null)
				&& (!(jobId.toLowerCase().contains("error")))
				&& (!(jobId.toLowerCase().contains("fatal")) && (!(jobId
						.toLowerCase().contains("failed"))))) {

			scoutBean.setGridJobId(jobId);
			getDbScouts().firstUpdateScout(scoutBean);

		}

		scoutJDL.delete();
		// DebugLog.log("jobId" + jobId);

		// }
		return jobId;
	}

	private File generateScoutJDL(ScoutInfo scoutBean) {

		// File scoutJDL;

		long submitTimeMillis = System.currentTimeMillis();
		scoutBean.setSubmittingTime(submitTimeMillis);

		StringBuffer content = new StringBuffer();

		int scoutId = scoutBean.getId();

		// DebugLog.log(submitTime);
		content.append("[\n");
		content.append("Type = \"Job\";\n");
		content.append("JobType = \"Normal\";\n");
		content.append("Executable = \"runScout.sh\";\n");
		content.append("InputSandbox = {\""
				+ Constants.scriptDir.getAbsolutePath() + "/runScout.sh\", \""
				+ Constants.scriptDir.getAbsolutePath() + "/scout.jar\"};\n");
		content.append("Arguments = \"" + scoutId + " " + submitTimeMillis
				+ " " + scoutBean.getCeId() + "\";\n");
		content.append("StdOutput = \"" + scoutId + ".out\";\n");
		content.append("StdError = \"" + scoutId + ".err\";\n");
		content.append("OutputSandbox = {\"" + scoutId + ".out\", \"" + scoutId
				+ ".err\"};\n");
		content.append("OutputSandboxBaseDestUri = \"gsiftp://localhost\";\n");
		content.append("]");
		
		//System.out.println("content ===== " + content);
		
		File scoutJDL = new File(Constants.tmpDir, "scout." + scoutId + ".jdl");
		try {
			PrintStream ps = new PrintStream(scoutJDL);
			ps.println(content);
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
			// logger.error("Failed to Generate Submit JDL: " + e.getMessage());
		}

		return scoutJDL;
	}

	public String delegateProxy(CeInfo ceBean) {

		String parts[] = ceBean.getName().split("/");
		String delegationId = "id_" + ceBean.getName();
		// DebugLog.log(delegationId);
		List<String> comm1 = new ArrayList<String>();
		comm1.add("glite-ce-delegate-proxy");
		comm1.add("-e");
		comm1.add(parts[0]);
		comm1.add(delegationId);

		List<String> comm2 = new ArrayList<String>();
		comm2.add("glite-ce-proxy-renew");
		comm2.add("-e");
		comm2.add(parts[0]);
		comm2.add(delegationId);
		DebugLog.log("renew proxy");
		ProcessBuilder builder1 = new ProcessBuilder(comm1);
		DebugLog.log(comm1);
		ProcessBuilder builder2 = new ProcessBuilder(comm2);
		DebugLog.log(comm2);

		try {
			Process p1 = builder1.start();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			// e1.printStackTrace();
			try {
				Process p2 = builder2.start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return delegationId;
	}

	// protected void startEmitters() {
	// EmitterResubmissionThread[] emitterResubmissionThreads = new
	// EmitterResubmissionThread[Constants.numEmitterResubmissionThread];
	// for (int i = 0; i < emitterResubmissionThreads.length; i++) {
	// DebugLog.log(i);
	// emitterResubmissionThreads[i] = new EmitterResubmissionThread(""
	// + i);
	// emitterResubmissionThreads[i].start();
	// try {
	// Thread.sleep(1000);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// ScoutPicker scoutPicker = new ScoutPicker();
	// scoutPicker.start();
	// while (true) {
	//
	// if (getEmitterThreads().size() < Constants.numEmitterThread) {
	// DebugLog.log("# Emitters: " + emitterThreads.size());
	// EmitterThread emitterThread = new EmitterThread(""
	// + System.currentTimeMillis());
	// getEmitterThreads().add(emitterThread);
	// emitterThread.start();
	//
	// }
	// try {
	// Thread.sleep(20000);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	//
	// }

	// private void moveScoutBeanToQueue(ScoutBean scoutBean, String queueName)
	// {
	// QueueUtils queueUtils = new QueueUtils();
	// queueUtils.sendSingleScoutBeanToQueue(scoutBean, queueName);
	// }

	// private void pickScoutForCe() {
	// try {
	//
	// int ceId = -1;
	// ActiveMQConnectionFactory connectionFactory = new
	// ActiveMQConnectionFactory(
	// Constants.activemqUrl);
	// Connection connection = connectionFactory.createConnection();
	// connection.start();
	// connection.setExceptionListener(this);
	// Session session = connection.createSession(false,
	// Session.AUTO_ACKNOWLEDGE);
	//
	// Destination destination = session
	// .createQueue(Constants.scoutWaitingQueue);
	// MessageConsumer consumer = session.createConsumer(destination);
	// Message message = consumer.receive(1000);
	// if (message instanceof ObjectMessage) {
	// ObjectMessage scoutBeanMessage = (ObjectMessage) message;
	// ScoutBean scoutBean = (ScoutBean) scoutBeanMessage.getObject();
	// ceId = scoutBean.getCeId();
	// moveScoutBeanToQueue(scoutBean, Constants.scoutReadyQueue);
	// }
	//
	// consumer.close();
	//
	// session.close();
	// connection.close();
	// getQueueUtils().pickScoutForCe(ceId, Constants.scoutWaitingQueue,
	// Constants.scoutReadyQueue);
	//
	// } catch (Exception e) {
	// DebugLog.log("Caught: " + e);
	// e.printStackTrace();
	// }
	// }

	public static void main(String[] args) {

		ScoutEmitter emitter = new ScoutEmitter();
		// emitter.startEmitters();
		// ScoutBean scoutBean = new ScoutBean(1, 1);
		// ScoutBean scoutBean2 = new ScoutBean(2, 0);
		// DebugLog.log(emitter.findVoForScout(scoutBean));
		//
		// DebugLog.log(emitter.findVoForScout(scoutBean2));
		// emitter.generateScoutJDL(scoutBean);
	}

	public DbScout getDbScouts() {
		return dbScouts;
	}

	public void setDbScouts(DbScout scoutDAO) {
		this.dbScouts = scoutDAO;
	}

	public DbJoin getDbJoin() {
		return dbJoin;
	}

	public void setDbJoin(DbJoin joinDAO) {
		this.dbJoin = joinDAO;
	}

	public Map<Integer, String> getCeIdVoName() {
		return ceIdVoName;
	}

	public void setCeIdVoName(Map<Integer, String> ceIdVoName) {
		this.ceIdVoName = ceIdVoName;
	}

	public Map<Integer, String> getCeIdCeName() {
		return ceIdCeName;
	}

	public void setCeIdCeName(Map<Integer, String> ceIdCeName) {
		this.ceIdCeName = ceIdCeName;
	}

	// public QueueUtils getQueueUtils() {
	// return queueUtils;
	// }
	//
	// public void setQueueUtils(QueueUtils queueUtils) {
	// this.queueUtils = queueUtils;
	// }

	public List<CeInfo> getCes() {
		return ces;
	}

	public void setCes(List<CeInfo> ces) {
		this.ces = ces;
	}

	public DbCe getCeDAO() {
		return dbCes;
	}

	public void setCeDAO(DbCe ceDAO) {
		this.dbCes = ceDAO;
	}

	// public class EmitterThread extends Thread implements ExceptionListener {
	//
	// private String emitterName;
	//
	// private String getEmitterName() {
	// return emitterName;
	// }
	//
	// private void setEmitterName(String emitterName) {
	// this.emitterName = emitterName;
	// }
	//
	// public EmitterThread(String name) {
	// // TODO Auto-generated constructor stub
	// setEmitterName(name);
	// }
	//
	// @Override
	// public void run() {
	// // TODO Auto-generated method stub
	//
	// ScoutBean tmpScoutBean = null;
	// // ScoutDAO scoutDAO = new ScoutDAOImpl();
	// String jobId = "";
	// boolean singleSubmissionError = false;
	// boolean ceError = false;
	// while (!singleSubmissionError) {
	// try {
	// ceError = false;
	// ActiveMQConnectionFactory connectionFactory = new
	// ActiveMQConnectionFactory(
	// Constants.activemqUrl);
	// Connection connection = connectionFactory
	// .createConnection();
	// connection.start();
	// connection.setExceptionListener(this);
	// Session session = connection.createSession(false,
	// Session.AUTO_ACKNOWLEDGE);
	//
	// Destination destination = session
	// .createQueue(Constants.scoutReadyQueue);
	// MessageConsumer consumer = session
	// .createConsumer(destination);
	// Message message = consumer.receive(5000);
	// if (message instanceof ObjectMessage) {
	// ObjectMessage scoutBeanMessage = (ObjectMessage) message;
	// ScoutBean scoutBean = (ScoutBean) scoutBeanMessage
	// .getObject();
	// tmpScoutBean = scoutBean;
	// // DebugLog.log("ScoutId: " + scoutBean.getId() +
	// // " Dest: " + scoutBean.getCe());
	// jobId = submitSingleScout(scoutBean);
	// DebugLog.log(jobId);
	// if (jobId == null) {
	// ceError = true;
	// singleSubmissionError = false;
	// } else if (jobId.toLowerCase().contains("error")
	// || jobId.toLowerCase().contains("fatal")) {
	// DebugLog.log(jobId);
	// singleSubmissionError = true;
	// for (String str : Constants.ceErrorPatterns) {
	// if (jobId.toLowerCase().contains(
	// str.toLowerCase())) {
	// ceError = true;
	// singleSubmissionError = false;
	// }
	// }
	//
	// }
	// if (ceError == true) {
	// QueueUtils queueUtils = new QueueUtils();
	// queueUtils.removeScoutsNotYetSubmitted(scoutBean
	// .getCeId());
	// }
	//
	// }
	// consumer.close();
	//
	// session.close();
	// connection.close();
	//
	// } catch (Exception e) {
	// DebugLog.log("Caught: " + e);
	// e.printStackTrace();
	// }
	//
	// }
	//
	// moveScoutBeanToQueue(tmpScoutBean, Constants.scoutResubmissionQueue);
	// Iterator<EmitterThread> iterator = getEmitterThreads().iterator();
	// // DebugLog.log("# existing emitters: " +
	// // getEmitterThreads().size());
	// // DebugLog.log("This name: " + this.emitterName);
	// while (iterator.hasNext()) {
	// // DebugLog.log("Iter name in run: " +
	// // iterator.next().getEmitterName());
	// if (iterator.next().getEmitterName().equals(getEmitterName())) {
	// DebugLog.log("Found emitter thread. Will now remove");
	// iterator.remove();
	// }
	// }
	// // DebugLog.log("# emitters after removed: " +
	// // getEmitterThreads().size());
	//
	// // DebugLog.log(jobId);
	//
	// }
	//
	// @Override
	// public void onException(JMSException arg0) {
	// // TODO Auto-generated method stub
	// DebugLog.log("JMS Exception occured.  Shutting down client.");
	// }
	//
	// }
	//
	// public class EmitterResubmissionThread extends Thread implements
	// ExceptionListener {
	//
	// private String name;
	//
	// public EmitterResubmissionThread(String name) {
	// // TODO Auto-generated constructor stub
	// this.name = name;
	// }
	//
	// @Override
	// public void run() {
	// // TODO Auto-generated method stub
	// String jobId = "";
	// while (true) {
	// try {
	// ActiveMQConnectionFactory connectionFactory = new
	// ActiveMQConnectionFactory(
	// Constants.activemqUrl);
	// Connection connection = connectionFactory
	// .createConnection();
	// connection.start();
	// connection.setExceptionListener(this);
	// Session session = connection.createSession(false,
	// Session.AUTO_ACKNOWLEDGE);
	// Destination destination = session
	// .createQueue(Constants.scoutResubmissionQueue);
	// MessageConsumer consumer = session
	// .createConsumer(destination);
	// Message message = consumer.receive(5000);
	// if (message instanceof ObjectMessage) {
	// ObjectMessage scoutBeanMessage = (ObjectMessage) message;
	// ScoutBean scoutBean = (ScoutBean) scoutBeanMessage
	// .getObject();
	// // DebugLog.log("ScoutId: " + scoutBean.getId() +
	// // " Dest: " + scoutBean.getCe());
	// jobId = submitSingleScout(scoutBean);
	// DebugLog.log(jobId);
	// } else {
	// // DebugLog.log("Received: " + message);
	// }
	//
	// consumer.close();
	// session.close();
	// connection.close();
	// } catch (Exception e) {
	// DebugLog.log("Caught: " + e);
	// e.printStackTrace();
	// }
	// }
	//
	// }
	//
	// @Override
	// public void onException(JMSException arg0) {
	// // TODO Auto-generated method stub
	// DebugLog.log("JMS Exception occured.  Shutting down client.");
	// }
	//
	// }
	//
	// public class ScoutPicker extends Thread {
	//
	// @Override
	// public void run() {
	// while (true) {
	// // if (getQueueUtils().isEmptyQueue(Constants.scoutReadyQueue)) {
	// // pickScoutForCe();
	// // }
	// if (getQueueUtils().getQueueSize(Constants.scoutReadyQueue) < 100){
	// pickScoutForCe();
	// }
	// try {
	// Thread.sleep(1000);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	//
	// }
	// }
	//
	// @Override
	// public void onException(JMSException arg0) {
	// // TODO Auto-generated method stub
	//
	// }
}
