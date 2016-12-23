package org.kisti.htc.dbmanager;

import org.kisti.htc.scoutmanager.CeInfo;
import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.DebugLog;

public class DbCeStatistic {

	private DbManager dbUtils;

	public DbCeStatistic() {
		// TODO Auto-generated constructor stub
		setDbUtils(new DbManager());
	}

	public void addScoutInfotoCeStatistic(CeInfo ceBean) {
		// TODO Auto-generated method stub
		// String sql = "insert into " + Constants.ceStatisticTable +
		// " values ("
		// + "\"" + ceBean.getName() + "\"" + ", " + ceBean.getResetTime()
		// + ", " + ceBean.getAtRound() + ", "
		// + ceBean.getNumSuccessSubmittedScout() + ", "
		// + ceBean.getSuccessRate() + ", " + ceBean.getAvgResponseTime()
		// + ")";

		String sql = "insert into " + Constants.ceStatisticTable
				+ " values (\""
				+ getDbUtils().convertLongToDate(ceBean.getResetTime())
				+ "\", " + ceBean.getId() + ", " + ceBean.getAtRound() + ", "
				+ ceBean.getNumSuccessSubmittedScout() + ", "
				+ ceBean.getNumArrivedScout() + ", "
				+ ceBean.getAvgResponseTime() + ", "
				+ "\""+ ceBean.getName() + "\"" +")";DebugLog.logSql(sql);

		// System.out.println(sql);
		getDbUtils().runCommand(sql);
	}

	public void updateScoutInfoToDb(CeInfo ceBean, long oldResetTime) {
		// TODO Auto-generated method stub
		// String sql = "update " + Constants.ceStatisticTable
		// + " set numSubmittedScout = "
		// + ceBean.getNumSuccessSubmittedScout() + ", round = "
		// + ceBean.getAtRound() + ", successRate = "
		// + ceBean.getSuccessRate() + ", avgResponseTime = "
		// + ceBean.getAvgResponseTime() + " where name = " + "\""
		// + ceBean.getName() + "\"" + " and resetTime = "
		// + ceBean.getResetTime();

		String sql = "update " + Constants.ceStatisticTable
				+ " set resetTime = \"" + getDbUtils().convertLongToDate(ceBean.getResetTime()) + "\", numSuccessSubmittedScout = "
				+ ceBean.getNumSuccessSubmittedScout() + ",name = " 
				+ "\""+ ceBean.getName() + "\"" + ", numArrivedScout = "
				+ ceBean.getNumArrivedScout() + ", avgResponseTime = "
				+ ceBean.getAvgResponseTime() + " where resetTime = \""
				+ getDbUtils().convertLongToDate(oldResetTime) 
				+ "\" and ceId = " + ceBean.getId() + " and atRound = "
				+ ceBean.getAtRound();DebugLog.logSql(sql);

		getDbUtils().runCommand(sql);
	}

	public DbManager getDbUtils() {
		return dbUtils;
	}

	public void setDbUtils(DbManager dbUtils) {
		this.dbUtils = dbUtils;
	}

	public static void main(String[] args) {
		DbCeStatistic ceStatisticDAO = new DbCeStatistic();
		CeInfo ceBean = new CeInfo(0, "darthvader", 0);
		ceBean.setResetTime(System.currentTimeMillis());
		ceStatisticDAO.addScoutInfotoCeStatistic(ceBean);DebugLog.logCestatistic("-------------insert---------");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ceBean.setNumSuccessSubmittedScout(100);
//		ceStatisticDAO.updateCeStatistic(ceBean);
	}

}
