package org.kisti.htc.dbmanager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.DebugLog;
import org.kisti.htc.scoutmanager.VoInfo;

public class DbVo {

	private DbManager dbUtils;

	public DbVo() {
		// TODO Auto-generated constructor stub
		setDbUtils(new DbManager());
	}

	public void insertVo(String voName) {
		// TODO Auto-generated method stub
		String sql = "insert into " + Constants.voTable + " (name) values (\""
				+ voName + "\")";DebugLog.logSql(sql);
		DbManager dbUtils = new DbManager();
		dbUtils.runCommand(sql);
	}

	public String getVoName(int voId) {
		// TODO Auto-generated method stub
		String sql = "select * from " + Constants.voTable + " where id = "
				+ voId;DebugLog.logSql(sql);DebugLog.logSql(sql);
		// System.out.println(sql);
		String voName = null;
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				voName = rs.getString("name");
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return voName;
	}

	private DbManager getDbUtils() {
		return dbUtils;
	}

	private void setDbUtils(DbManager dbUtils) {
		this.dbUtils = dbUtils;
	}

	public List<VoInfo> collectVos() {
		// TODO Auto-generated method stub
		List<VoInfo> vos = new ArrayList<VoInfo>();
		String sql = "select * from " + Constants.voTable;DebugLog.logSql(sql);
//		System.out.println(sql);
		Connection connection = getDbUtils().getConnection();
		PreparedStatement preStmt = null;
		try {
			preStmt = connection.prepareStatement(sql);
			ResultSet rs = preStmt.executeQuery();

			while (rs.next()) {
				VoInfo vo = new VoInfo(rs.getInt("id"), rs.getString("name"));
				vos.add(vo);
				DebugLog.logSql("---------------");System.out.println(rs.getInt("id")+ rs.getString("name"));
			}
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vos;
	}

}
