package org.kisti.htc.scoutmanager;

/*
 * +--------------------------+-------------+------+-----+---------+-------+
 | Field                    | Type        | Null | Key | Default | Extra |
 +--------------------------+-------------+------+-----+---------+-------+
 | id                       | int(11)     | NO   | PRI | 0       |       |
 | name                     | varchar(50) | YES  |     | NULL    |       |
 | voId                     | int(11)     | YES  | MUL | NULL    |       |
 | atRound                  | int(11)     | YES  |     | NULL    |       |
 | resetTime                | datetime    | YES  |     | NULL    |       |- converted
 | numSuccessSubmittedScout | int(11)     | YES  |     | NULL    |       |
 | numArrivedScout          | int(11)     | YES  |     | NULL    |       |
 | firstScoutArrivalTime    | datetime    | YES  |     | NULL    |       |- converted
 | avgResponseTime          |   mediumtext| YES  |     | NULL    |       |
 | allowMoreScouts          | tinyint(1)  | YES  |     | NULL    |       |
 +--------------------------+-------------+------+-----+---------+-------+

 */

public class CeInfo {

	private int id;
	private String name;
	private int voId;
	private int cpu;
	private int free;
	private int total;
	private int running;
	private int waiting;
	private int atRound = 0;
	private long resetTime;
	private int numSuccessSubmittedScout = 0;
	private int numArrivedScout = 0;
	private long firstScoutArrivalTime;
	private long avgResponseTime = 0;
	private boolean allowMoreScouts = false;
	

	// private long avgLifeTime = 0;
	// private double successRate = 0;

	public CeInfo(int id, String name, int voId) {
		setId(id);
		setName(name);
		setVoId(voId);
	}

	// public void reset(){
	// setResetTime(System.currentTimeMillis());
	// // setNumSuccessSubmittedScout(0);
	// // setNumArrivedScout(0);
	// setFirstScoutArrivalTime(0);
	// }
	@Override
	public String toString() {
		// TODO Auto-generated method stub

		String str = "ceId: " + getId() + ", name: " + getName() + ", round: "
				+ getAtRound() + ", numSuccessSubmitted: "
				+ getNumSuccessSubmittedScout() + ", numArrivedScout: "
				+ getNumArrivedScout() + ", avgResponseTime: "
				+ getAvgResponseTime();

		return str;
	}

	public String toStringForClient() {
		String str = "name: " + getName() + ", measuredTime: " + getResetTime()
				+ ", numMeasuredFreeCores: " + getNumArrivedScout()
				+ ", avgResponseTime: " + getAvgResponseTime();
		return str;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getAvgResponseTime() {
		return avgResponseTime;
	}

	public void setAvgResponseTime(long avgResponseTime) {
		this.avgResponseTime = avgResponseTime;
	}

	public double getSuccessRate() {
		double successRate = 0;
		if (getNumSuccessSubmittedScout() != 0) {
			successRate = ((double) getNumArrivedScout())
					/ ((double) getNumSuccessSubmittedScout());
		}
		return successRate;
	}

	public int getAtRound() {
		return atRound;
	}

	public void setAtRound(int atRound) {
		this.atRound = atRound;
	}

	public boolean isAllowMoreScouts() {
		return allowMoreScouts;
	}

	public void setAllowMoreScouts(boolean allowMoreScouts) {
		this.allowMoreScouts = allowMoreScouts;
	}

	public long getScoutFirstArivalTime() {
		return firstScoutArrivalTime;
	}

	public void setFirstScoutArrivalTime(long firstScoutArrivalTime) {
		this.firstScoutArrivalTime = firstScoutArrivalTime;
	}

	public long getResetTime() {
		return resetTime;
	}

	public void setResetTime(long resetTime) {
		this.resetTime = resetTime;
	}

	public int getNumSuccessSubmittedScout() {
		return numSuccessSubmittedScout;
	}

	public void setNumSuccessSubmittedScout(int numSuccessSubmittedScout) {
		this.numSuccessSubmittedScout = numSuccessSubmittedScout;
	}

	public int getNumArrivedScout() {
		return numArrivedScout;
	}

	public void setNumArrivedScout(int numArrivedScout) {
		this.numArrivedScout = numArrivedScout;
	}

	public int getVoId() {
		return voId;
	}

	public void setVoId(int voId) {
		this.voId = voId;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCpuNum() {
		return cpu;
	}

	public void setCpuNum(int cpu) {
		this.cpu = cpu;
	}

	public int getFreeNum() {
		return free;
	}

	public void setFreeNum(int free) {
		this.free = free;
	}

	public int getTotalNum() {
		return total;
	}

	public void setTotalNum(int total) {
		this.total = total;
	}

	public int getRunningNum() {
		return running;
	}

	public void setRunningNum(int running) {
		this.running = running;
	}

	public int getWaitingNum() {
		return waiting;
	}

	public void setWaitingNum(int waiting) {
		this.waiting = waiting;
	}
}
