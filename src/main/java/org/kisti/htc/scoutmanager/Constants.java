package org.kisti.htc.scoutmanager;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Constants {

	// public static final int numScoutPerCe = 10;
	// public static final int numScoutPerCe = 500;

	public static final Map<Integer, Integer> numScoutPerCePerRound = new HashMap<Integer, Integer>() {
		{
			put(0, 50);
			//put(1, 25);
			//put(2, 5);
			//put(3, 41);
			//put(4, 81);
			//put(5, 121);
			//put(6, 221);
		}
	};

	public static final double successRateThreshold = 0.1;

	public static final List<VoInfo> vos = Arrays.asList(
	// new VoBean("vo.france-asia.org"),
			new VoInfo("biomed"));
	public static final File gliteDir = new File("conf/glite");
	// public static final String voName = "vo.france-asia.org";
	// public static final String voName = "biomed";
	// public static final File proxyFile = new File(gliteDir, voName +
	// ".proxy");
	// public static final File matchJDL = new File(gliteDir, voName +
	// ".match.jdl");
	public static final File scoutJDL = new File(gliteDir, "scout.jdl");
	public static final File tmpDir = new File("tmp");
	public static final File scriptDir = new File("scripts");

	// public static final List<CeBean> cesVofa = Arrays.asList(
	// // new CeBean("darthvader.kisti.re.kr:8443/cream-pbs-vofa")
	// new CeBean("cccreamceli09.in2p3.fr:8443/cream-sge-long",
	// "vo.france-asia.org"),
	// new CeBean("cccreamceli09.in2p3.fr:8443/cream-sge-medium",
	// "vo.france-asia.org"),
	// new CeBean("cccreamceli10.in2p3.fr:8443/cream-sge-long",
	// "vo.france-asia.org"),
	// new CeBean("cccreamceli10.in2p3.fr:8443/cream-sge-medium",
	// "vo.france-asia.org"),
	// // new CeBean("cce.ihep.ac.cn:8443/cream-pbs-france_asia.org",
	// "vo.france-asia.org"),
	// new CeBean("kek2-ce01.cc.kek.jp:8443/cream-lsf-gridmiddle",
	// "vo.france-asia.org"),
	// new CeBean("kek2-ce02.cc.kek.jp:8443/cream-lsf-gridmiddle",
	// "vo.france-asia.org"),
	// new CeBean("lpnhe-cream.in2p3.fr:8443/cream-pbs-france_asia.org",
	// "vo.france-asia.org"),
	// new CeBean("marcream01.in2p3.fr:8443/cream-pubs-asia",
	// "vo.france-asia.org"),
	// new CeBean("marcream02.in2p3.fr:8443/cream-pbs-asia",
	// "vo.france-asia.org")
	// );

	// public static final List<CeBean> Ces = Arrays.asList(
	// new CeBean("darthvader.kisti.re.kr:8443/cream-pbs-biomed"),
	// new CeBean("marcream01.in2p3.fr:8443/cream-pbs-biomed")
	// new CeBean("marcream02.in2p3.fr:8443/cream-pbs-biomed")
	// );

	// public static final List<CeBean> Ces = Arrays.asList(new
	// CeBean("darthvader.kisti.re.kr:8443/cream-pbs-vofa"));

	// public static final int scoutPort = 9101;
	// public static final String reportURL =
	// "http://127.0.0.1:9102/ReportServer";
	public static final String reportURL = "http://150.183.250.128:9200/ReportServer";

	public static final String scoutDb = "jdbc:mysql://127.0.0.1/htcaas_scout";
	public static final String scoutTable = "scoutinfo";
	public static final String ceStatisticTable = "cestatistic_nowait";
	public static final String ceTable = "ce_top10_gridinfo";
	public static final String infositesTable = "infosites";
	public static final String voTable = "vo";

	public static final int numEmitterPrimary = 40;
	public static final int numEmitterSecondary = 5;
	public static final int numScoutServer = 10;
	public static final int numScoutRecorder = 10;
	public static final long maxWaitTime = 30 * 60000;// 15 minutes''28/01 changed 15->30

	public static final String activemqUrl = "tcp://127.0.0.1:61616";
	public static final String JMXServiceURL = "service:jmx:rmi:///jndi/rmi://150.183.250.128:2011/jmxrmi";
	public static final String objectName = "my-broker:BrokerName=localhost,Type=Broker";
	public static final String scoutWaitingQueue = "scoutWaitingQueue";
	public static final String scoutReadyQueue = "scoutReadyQueue";
	public static final String scoutResubmissionQueue = "scoutResubmissionQueue";

	public static final long estimatedClusterJobAllocatingTime = 20 * 60000;// 60
																			// minutes

	public static final List<String> singleSubmissionErrorPatterns = Arrays
			.asList("socket timeout", "sending file", "connection timed out",
					"submitting JDL");

	public static final List<String> ceErrorPatterns = Arrays.asList(
			"CREAMDelegationService not available",
			"The CREAM service cannot accept jobs at the moment",
			"gridsite-delegation");

	// public static int scoutId = 0;

}
