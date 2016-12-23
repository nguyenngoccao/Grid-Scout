package org.kisti.htc.dbmanager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.kisti.htc.scoutmanager.CeInfo;
import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.DebugLog;
import org.kisti.htc.scoutmanager.VoInfo;

/*
 * +--------------------------+-------------+------+-----+---------+-------+
 | Field                    | Type        | Null | Key | Default | Extra |
 +--------------------------+-------------+------+-----+---------+-------+
 | id                       | int(11)     | NO   | PRI | 0       |       |
 | name                     | varchar(50) | YES  |     | NULL    |       |
 | voId                     | int(11)     | YES  | MUL | NULL    |       |
 | atRound                  | int(11)     | YES  |     | NULL    |       |
 | resetTime                | datetime    | YES  |     | NULL    |       |
 | numSuccessSubmittedScout | int(11)     | YES  |     | NULL    |       |
 | numArrivedScout          | int(11)     | YES  |     | NULL    |       |
 | firstScoutArrivalTime    | datetime    | YES  |     | NULL    |       |
 | avgResponseTime          | int(11)     | YES  |     | NULL    |       |
 | allowMoreScouts          | tinyint(1)  | YES  |     | NULL    |       |
 +--------------------------+-------------+------+-----+---------+-------+

 */

public class DbCe {

	private DbManager dbUtils;

	public DbCe() {
		// TODO Auto-generated constructor stub
		setDbUtils(new DbManager());

	}

	public void updateCeInfoToDb(CeInfo ceBean) {
		// TODO Auto-generated method stub

		// System.out.println("GregorianCalendar -"+sdf.format(calendar.getTime()));

		String sql;
		if (ceBean.getScoutFirstArivalTime() == 0) {
			sql = "update " + Constants.ceTable + " set atRound = "
					+ ceBean.getAtRound() + ", resetTime = \""
					+ getDbUtils().convertLongToDate(ceBean.getResetTime())
					+ "\", numSuccessSubmittedScout = "
					+ ceBean.getNumSuccessSubmittedScout()
					+ ", numArrivedScout = " + ceBean.getNumArrivedScout()
					+ ", firstScoutArrivalTime = NULL , avgResponseTime = "
					+ ceBean.getAvgResponseTime() + ", allowMoreScouts = "
					+ ceBean.isAllowMoreScouts() + " where id = "
					+ ceBean.getId();
		} else {
			sql = "update "
					+ Constants.ceTable
					+ " set atRound = "
					+ ceBean.getAtRound()
					+ ", resetTime = \""
					+ getDbUtils().convertLongToDate(ceBean.getResetTime())
					+ "\", numSuccessSubmittedScout = "
					+ ceBean.getNumSuccessSubmittedScout()
					+ ", numArrivedScout = "
					+ ceBean.getNumArrivedScout()
					+ ", firstScoutArrivalTime = \""
					+ getDbUtils().convertLongToDate(
							ceBean.getScoutFirstArivalTime())
					+ "\", avgResponseTime = " + ceBean.getAvgResponseTime()
					+ ", allowMoreScouts = " + ceBean.isAllowMoreScouts()
					+ " where id = " + ceBean.getId();
		}

		// System.out.println(sql);
		DbManager dbUtils = new DbManager();
		dbUtils.runCommand(sql);

	}

	public void initCeInfoInDb(CeInfo ceBean) {
		
		String sql;

		sql = "update " + Constants.ceTable + " set "
				+ "  cpu = "		+ ceBean.getCpuNum()			
				+ ", free = "		+ ceBean.getFreeNum()
				+ ", total = "		+ ceBean.getTotalNum()
				+ ", running = "	+ ceBean.getRunningNum()
				+ ", waiting = "	+ ceBean.getWaitingNum()				
				+ " where id = " + ceBean.getId();
		
		DbManager dbUtils = new DbManager();
		dbUtils.runCommand(sql);
		System.out.println(sql);
	}

	public void insertCeToDb(CeInfo ceBean) {
		// TODO Auto-generated method stub
		String sql = "insert into " + Constants.ceTable
				+ " (id, name, voId) values (" + ceBean.getId() + ", \""
				+ ceBean.getName() + "\"" + ", " + ceBean.getVoId() + ")";
		DebugLog.logSql(sql);
		// System.out.println(sql);
		DbManager dbUtils = new DbManager();
		dbUtils.runCommand(sql);
	}

	public List<CeInfo> collectCes() {
		// TODO Auto-generated method stub
		DebugLog.log("-----------");
		List<CeInfo> ces = new ArrayList<CeInfo>();
		String sql = "select * from " + Constants.ceTable;
			//	+ " where numArrivedScout > 2";
		DebugLog.logSql(sql);
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				CeInfo ce = new CeInfo(rs.getInt("id"), rs.getString("name"),
						rs.getInt("voId"));
				ces.add(ce);
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ces;
	}

	private DbManager getDbUtils() {
		return dbUtils;
	}

	private void setDbUtils(DbManager dbUtils) {
		this.dbUtils = dbUtils;
	}

	public String findCeNameFromId(int ceId) {
		// TODO Auto-generated method stub
		String ceName = null;
		String sql = "select * from " + Constants.ceTable + " where id = "
				+ ceId;
		DebugLog.logSql(sql);
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				ceName = rs.getString("name");
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ceName;
	}

	public int findCeIdAtRound(int ceId) {
		// TODO Auto-generated method stub
		int atRound = -2;
		String sql = "select atRound from " + Constants.ceTable
				+ " where Id = " + ceId;
		DebugLog.logSql(sql);
		DebugLog.logSql(sql);
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				atRound = rs.getInt("atRound");
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return atRound;
	}

	// @Override
	// public CeBean getCeInfo(String ceName, String voName) {
	// // TODO Auto-generated method stub
	// CeBean ceBean = null;
	//
	// String sql = "select * from " + Constants.ceTable + " where name = \"" +
	// ceName + "\"";DebugLog.logSql(sql);
	// Connection connection = getDbUtils().getConnection();
	// PreparedStatement preStmt = null;
	//
	//
	// try {
	// preStmt = connection.prepareStatement(sql);
	// ResultSet rs = preStmt.executeQuery();
	//
	// while (rs.next()) {
	//
	// ceBean = new CeBean(rs.getInt("id"), ceName, rs.getInt("voId"));
	// ceBean.setAtRound(rs.getInt("atRound"));
	// System.out.println(rs.getString("resetTime"));
	// if (rs.getString("resetTime") != null){
	// ceBean.setResetTime(getDbUtils().convertDateToLong(rs.getString("resetTime")));
	// } else {
	// ceBean.setResetTime(0);
	// }
	// ceBean.setNumSuccessSubmittedScout(rs.getInt("numSuccessSubmittedScout"));
	// ceBean.setNumArrivedScout(rs.getInt("numArrivedScout"));
	// if (rs.getString("firstScoutArrivalTime") != null){
	// ceBean.setFirstScoutArrivalTime(getDbUtils().convertDateToLong(rs.getString("firstScoutArrivalTime")));
	// } else {
	// ceBean.setFirstScoutArrivalTime(0);
	// }
	// if (rs.getLong("avgResponseTime") != 0){
	// ceBean.setAvgResponseTime(rs.getLong("avgResponseTime"));
	// } else {
	// ceBean.setAvgResponseTime(0);
	// }
	// }
	// preStmt.close();
	// connection.close();
	// } catch (SQLException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return ceBean;
	// }

	public static void main(String[] args) {
		DbCe ceDAO = new DbCe();
		// System.out.println(ceDAO.findCeIdAtRound(1));
	}
}
