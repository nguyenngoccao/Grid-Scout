package org.kisti.htc.dbmanager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.print.attribute.standard.Finishings;

import org.kisti.htc.scoutmanager.CeInfo;
import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.DebugLog;
import org.kisti.htc.scoutmanager.ScoutInfo;

public class DbScout {

	private DbManager dbUtils;

	public DbScout() {
		// TODO Auto-generated constructor stub
		setDbUtils(new DbManager());
	}

	public void insertScout(ScoutInfo scoutBean) {
		// TODO Auto-generated method stub
		String sql = "insert into " + Constants.scoutTable
				+ "(id, ceId) values (" + scoutBean.getId() + ", "
				+ scoutBean.getCeId() + ")";DebugLog.logSql(sql);
		// System.out.println(sql);
		getDbUtils().runCommand(sql);
	}

	public void updateScoutInfoToDb(ScoutInfo scoutBean) {
		// TODO Auto-generated method stub

		String sql = "update " + Constants.scoutTable + " set wn = " + "\""
				+ scoutBean.getIpAddress() + "\"" + ", arrivalTime = \""
				+ getDbUtils().convertLongToDate(scoutBean.getArrivalTime())
				+ "\", lastReportTime = \""
				+ getDbUtils().convertLongToDate(scoutBean.getLastReportTime())
				+ "\" where id = " + scoutBean.getId();DebugLog.logSql(sql);

		getDbUtils().runCommand(sql);
	}

	public void deleteScout(int scoutId) {
		// TODO Auto-generated method stub

		String sql = "delete from " + Constants.scoutTable + " where id ="
				+ scoutId;DebugLog.logSql(sql);
		getDbUtils().runCommand(sql);
	}

	public void deleteAllScoutsInScoutTable() {
		// TODO Auto-generated method stub
		String sql = "truncate " + Constants.scoutTable;DebugLog.logSql(sql);
		getDbUtils().runCommand(sql);
	}

	public void getScoutInfoFromDb(CeInfo ceBean) {
		// TODO Auto-generated method stub
		countNumSuccessSubmittedScouts(ceBean);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
				Locale.US);
		int numArrivedScouts = 0;
		long totalResponseTimeMilli = 0;
		long totalLifeTimeMilli = 0;
		long firstScoutArrivalTime = Long.MAX_VALUE;

		String sql = "select * from " + Constants.scoutTable + " where ceId = "
				+ ceBean.getId() + " and lastReportTime != \"null\"";DebugLog.logSql(sql);

		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;

		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				long arrivalTimeMilli =getDbUtils().convertDateToLong(rs.getString("arrivalTime"));
				long submitTimeMilli = getDbUtils().convertDateToLong(rs.getString("submitTime"));
				long lastReportTimeMilli = getDbUtils().convertDateToLong(rs.getString("lastReportTime"));

				long tmpResponseTimeMilli = arrivalTimeMilli - submitTimeMilli;
				
				totalResponseTimeMilli += tmpResponseTimeMilli;
				long tmpLifeTimeMilli = lastReportTimeMilli - arrivalTimeMilli;
				totalLifeTimeMilli += tmpLifeTimeMilli;
				if (firstScoutArrivalTime > arrivalTimeMilli) {
					firstScoutArrivalTime = arrivalTimeMilli;
				}
				numArrivedScouts++;
			}

			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long avgResponseTime = 0;
		long avgLifeTime = 0;

		if (numArrivedScouts != 0) {
			avgResponseTime = totalResponseTimeMilli / numArrivedScouts;
			avgLifeTime = totalLifeTimeMilli / numArrivedScouts;
		}
		// System.out.println("SuccesRate = " + successRate);
		// ceBean.setSuccessRate(successRate);
		ceBean.setAvgResponseTime(avgResponseTime);
		ceBean.setNumArrivedScout(numArrivedScouts);
		if (firstScoutArrivalTime != Long.MAX_VALUE) {
			ceBean.setFirstScoutArrivalTime(firstScoutArrivalTime);
		}
		// ceBean.setAvgLifeTime(avgLifeTime);
		// System.out.println(numArrivedScouts);
	}


	public void deleteScoutInScoutTable(CeInfo ceBean) {
		// TODO Auto-generated method stub
		String sql = "delete from " + Constants.scoutTable + " where ceId = "
				+ ceBean.getId();DebugLog.logSql(sql);
		getDbUtils().runCommand(sql);
	}

	public DbManager getDbUtils() {
		return dbUtils;
	}

	public void setDbUtils(DbManager dbUtils) {
		this.dbUtils = dbUtils;
	}

	public void firstUpdateScout(ScoutInfo scoutBean) {
		// TODO Auto-generated method stub
		String sql = "update " + Constants.scoutTable + " set submitTime = \""
				+ getDbUtils().convertLongToDate(scoutBean.getSubmitTime())
				+ "\" ,gridJobId = " + "\"" + scoutBean.getGridJobId() + "\""
				+ " where id = " + scoutBean.getId();DebugLog.logSql(sql);

		getDbUtils().runCommand(sql);
	}

	private void countNumSuccessSubmittedScouts(CeInfo ceBean) {
		// TODO Auto-generated method stub
		String sql = "select * from " + Constants.scoutTable
				+ " where ceId = " + ceBean.getId() + " and submitTime != 0";DebugLog.logSql(sql);
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		int count = 0;
		long firstScoutSubmitTime = Long.MAX_VALUE;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();
			while (rs.next()) {
				if (firstScoutSubmitTime > getDbUtils().convertDateToLong(rs.getString("submitTime"))){
					firstScoutSubmitTime = getDbUtils().convertDateToLong(rs.getString("submitTime"));
				}
				count++;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ceBean.setNumSuccessSubmittedScout(count);
		if (firstScoutSubmitTime != Long.MAX_VALUE){System.out.println("firstScoutSubmitTime != Long.MAX_VALUE");
			ceBean.setResetTime(firstScoutSubmitTime);
		}
		
	}

	public int findMaxScoutIdFromScoutTable() {
		// TODO Auto-generated method stub
		int maxId = -1;
		String sql = "select max(id) from " + Constants.scoutTable;DebugLog.logSql(sql);
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();
			while (rs.next()) {
				maxId = rs.getInt("max(id)");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println("macId = " + maxId);
		return maxId;
	}
	
	public boolean isScoutSubmitted(ScoutInfo scoutBean) {
		// TODO Auto-generated method stub
		String submitTime = "";
		String sql = "select * from " + Constants.scoutTable + " where id = " + scoutBean.getId() + " and submitTime is not null";DebugLog.logSql(sql);
		Connection connection = getDbUtils().getConnection();DebugLog.logSql(sql);
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();
			while (rs.next()) {
				submitTime = rs.getString("submitTime");
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println("macId = " + maxId);
		return false;
		
	}
	
	public static void main(String[] args){
		DbScout scoutDAO = new DbScout();
		System.out.println(scoutDAO.findMaxScoutIdFromScoutTable());DebugLog.logScout("------");
	}

	

}
