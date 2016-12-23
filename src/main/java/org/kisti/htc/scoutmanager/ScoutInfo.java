package org.kisti.htc.scoutmanager;

import java.io.Serializable;

/*
 * +----------------+--------------+------+-----+---------+-------+
| Field          | Type         | Null | Key | Default | Extra |
+----------------+--------------+------+-----+---------+-------+
| id             | int(11)      | NO   | PRI | NULL    |       |
| ce             | varchar(100) | YES  |     | NULL    |       |
| wn             | varchar(15)  | YES  |     | NULL    |       |
| submitTime     | mediumtext   | NO   |     | NULL    |       |
| arrivalTime    | mediumtext   | YES  |     | NULL    |       |
| lastReportTime | mediumtext   | YES  |     | NULL    |       |
| jobId          | varchar(100) | NO   |     | NULL    |       |
+----------------+--------------+------+-----+---------+-------+

 */
public class ScoutInfo implements Serializable {

	private int jobId;
	private int ceId;
	private String ipAddress;
	private long submitTime;
	private long arrivalTime;
	private long lastReportTime;
	private String gridJobId;
	
	public ScoutInfo(int id, int ceId) {
		// TODO Auto-generated constructor stub
		setId(id);
		setCeId(ceId);
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String str;
		if (getGridJobId() != null){
			str = "scoutNo: " + getId() + ", ceId: " + getCeId() + ", wn: " + getIpAddress()
					+ ", arrivalTime: " + getArrivalTime()
					+ ", lastResponseTime: " + getLastReportTime()
					+ ", jobId: " + getGridJobId();
		} else { //jobId = null in scout report, in this case ignore jobId
			str = "scoutNo: " + getId() + ", ceId: " + getCeId() + ", wn: " + getIpAddress()
					+ ", submitTime: " + getSubmitTime()
					+ ", arrivalTime: " + getArrivalTime()
					+ ", lastResponseTime: " + getLastReportTime();
		}
		
		return str;
	}
	

	public int getId() {
		return jobId;
	}

	public void setId(int id) {
		this.jobId = id;
	}

	public int getCeId() {
		return ceId;
	}

	public void setCeId(int ceId) {
		this.ceId = ceId;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String wn) {
		this.ipAddress = wn;
	}

	public long getSubmitTime() {
		return submitTime;
	}

	public void setSubmittingTime(long submitTime) {
		this.submitTime = submitTime;
	}

	public long getArrivalTime() {
		return arrivalTime;
	}

	public void setArrivalTime(long arrivedTime) {
		this.arrivalTime = arrivedTime;
	}

	public long getLastReportTime() {
		return lastReportTime;
	}

	public void setLastResponseTime(long lastReportTime) {
		this.lastReportTime = lastReportTime;
	}

	public String getGridJobId() {
		return gridJobId;
	}

	public void setGridJobId(String gridJobId) {
		this.gridJobId = gridJobId;
	}
}
