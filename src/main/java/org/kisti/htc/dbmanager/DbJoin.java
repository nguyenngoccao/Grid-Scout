package org.kisti.htc.dbmanager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cxf.binding.corba.wsdl.Array;
import org.kisti.htc.scoutmanager.CeInfo;
import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.DebugLog;
import org.kisti.htc.scoutmanager.ScoutInfo;
import org.kisti.htc.scoutmanager.VoInfo;

public class DbJoin {

	private DbManager dbUtils;

	public DbJoin() {
		// TODO Auto-generated constructor stub
		setDbUtils(new DbManager());
	}

	public VoInfo findVoForCe(CeInfo ceBean) {
		// TODO Auto-generated method stub

		String sql = "select " + Constants.voTable + ".id, "
				+ Constants.voTable + ".name from " + Constants.ceTable
				+ " inner join " + Constants.voTable + " on "
				+ Constants.ceTable + ".voId = " + Constants.voTable
				+ ".id where " + Constants.ceTable + ".id = " + ceBean.getId();

		 System.out.println(sql);
		return (findVo(sql));
	}

	public DbManager getDbUtils() {
		return dbUtils;
	}

	public void setDbUtils(DbManager dbUtils) {
		this.dbUtils = dbUtils;
	}

	public VoInfo findVoForScout(ScoutInfo scoutBean) {
		// TODO Auto-generated method stub
		String sql = "select " + Constants.voTable + ".id, "
				+ Constants.voTable + ".name from " + Constants.scoutTable
				+ " inner join " + Constants.ceTable + " inner join "
				+ Constants.voTable + " on " + Constants.scoutTable
				+ ".ceId = " + Constants.ceTable + ".id and "
				+ Constants.ceTable + ".voId = " + Constants.voTable
				+ ".id and " + Constants.scoutTable + ".id = "
				+ scoutBean.getId();
		// System.out.println(sql);
		return (findVo(sql));
	}

	private VoInfo findVo(String sql) {
		VoInfo vo = null;
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;DebugLog.logSql(sql);
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				vo = new VoInfo(rs.getInt("id"), rs.getString("name"));
				
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vo;
	}

	public String findCeNameForScout(ScoutInfo scoutBean) {
		// TODO Auto-generated method stub
		String ceName = null;
		String sql = "select " + Constants.ceTable + ".name from "
				+ Constants.scoutTable + " inner join " + Constants.ceTable
				+ " on " + Constants.scoutTable + ".ceId = "
				+ Constants.ceTable + ".id and " + Constants.scoutTable
				+ ".id = " + scoutBean.getId();DebugLog.logSql(sql);
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

	public CeInfo getCeInfo(String ceName, String voName) {
		// TODO Auto-generated method stub
		CeInfo ceBean = null;
		String sql = "select " + Constants.ceTable + ".id, "
				+ Constants.ceTable + ".name, " + Constants.ceTable + ".voId, "
				+ Constants.ceTable + ".atRound, " + Constants.ceTable
				+ ".resetTime, " + Constants.ceTable
				+ ".numSuccessSubmittedScout, " + Constants.ceTable
				+ ".numArrivedScout, " + Constants.ceTable
				+ ".firstScoutArrivalTime, " + Constants.ceTable
				+ ".avgResponseTime" + " from " + Constants.ceTable
				+ " inner join " + Constants.voTable + " where "
				+ Constants.ceTable + ".name = \"" + ceName + "\" and "
				+ Constants.ceTable + ".voId = " + Constants.voTable
				+ ".id and " + Constants.voTable + ".name = \"" + voName + "\"";
//		System.out.println(sql);
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				ceBean = new CeInfo(rs.getInt("id"), rs.getString("name"), rs.getInt("voId"));
				ceBean.setAtRound(rs.getInt("atRound"));
//				System.out.println(rs.getString("resetTime"));
				if (rs.getString("resetTime") != null){
					ceBean.setResetTime(getDbUtils().convertDateToLong(rs.getString("resetTime")));
				} else {
					ceBean.setResetTime(0);
				}
				ceBean.setNumSuccessSubmittedScout(rs.getInt("numSuccessSubmittedScout"));	
				ceBean.setNumArrivedScout(rs.getInt("numArrivedScout"));
				if (rs.getString("firstScoutArrivalTime") != null){
					ceBean.setFirstScoutArrivalTime(getDbUtils().convertDateToLong(rs.getString("firstScoutArrivalTime")));
				} else {
					ceBean.setFirstScoutArrivalTime(0);
				}
				if (rs.getLong("avgResponseTime") != 0){
					ceBean.setAvgResponseTime(rs.getLong("avgResponseTime"));
				} else {
					ceBean.setAvgResponseTime(0);
				}
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ceBean;
	}
	
	public List<CeInfo> getAllVoInfo(String voName) {
		// TODO Auto-generated method stub
		List<CeInfo> ceBeans = new ArrayList<CeInfo>();
		String sql = "select " + Constants.ceTable + ".id, "
				+ Constants.ceTable + ".name, " + Constants.ceTable + ".voId, "
				+ Constants.ceTable + ".atRound, " + Constants.ceTable
				+ ".resetTime, " + Constants.ceTable
				+ ".numSuccessSubmittedScout, " + Constants.ceTable
				+ ".numArrivedScout, " + Constants.ceTable
				+ ".firstScoutArrivalTime, " + Constants.ceTable
				+ ".avgResponseTime" + " from " + Constants.ceTable
				+ " inner join " + Constants.voTable + " where "
				+ Constants.ceTable + ".voId = " + Constants.voTable
				+ ".id and " + Constants.voTable + ".name = \"" + voName + "\"";
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				CeInfo ceBean = new CeInfo(rs.getInt("id"), rs.getString("name"), rs.getInt("voId"));
				ceBean.setAtRound(rs.getInt("atRound"));
//				System.out.println(rs.getString("resetTime"));
				if (rs.getString("resetTime") != null){
					ceBean.setResetTime(getDbUtils().convertDateToLong(rs.getString("resetTime")));
				} else {
					ceBean.setResetTime(0);
				}
				ceBean.setNumSuccessSubmittedScout(rs.getInt("numSuccessSubmittedScout"));	
				ceBean.setNumArrivedScout(rs.getInt("numArrivedScout"));
				if (rs.getString("firstScoutArrivalTime") != null){
					ceBean.setFirstScoutArrivalTime(getDbUtils().convertDateToLong(rs.getString("firstScoutArrivalTime")));
				} else {
					ceBean.setFirstScoutArrivalTime(0);
				}
				if (rs.getLong("avgResponseTime") != 0){
					ceBean.setAvgResponseTime(rs.getLong("avgResponseTime"));
				} else {
					ceBean.setAvgResponseTime(0);
				}
				ceBeans.add(ceBean);
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ceBeans;
	}
	
	public static void main (String[] args){
		DbJoin joinDAO = new DbJoin();
//		String ceName = "cccreamceli09.in2p3.fr:8443/cream-sge-long";
		String voName = "biomed";
//		System.out.println((joinDAO.getCeInfo(ceName, voName)).toStringForClient());
		for (CeInfo ceBean: joinDAO.getAllVoInfo(voName)){
			//System.out.println(ceBean.toStringForClient());
		}
		
	}



}
