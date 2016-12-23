package org.kisti.htc.dbmanager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.DebugLog;
import org.kisti.htc.scoutmanager.ScoutInfo;

public class DbManager {
	public Connection getConnection() {

		String URL = Constants.scoutDb;
		String username = "root";
		String password = "kisti123";

		Connection connection = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(URL, username, password);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;

	}

	public void runCommand(String sql) {
		Connection connection = null;
		PreparedStatement preStmt = null;

		connection = getConnection();DebugLog.logSql(sql);
		try {
			preStmt = connection.prepareStatement(sql);
			preStmt.execute();
			preStmt.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//System.out.println(sql);
			e.printStackTrace();
		}

	}
	
	public String convertLongToDate(long dateMilisecs){
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);

	        GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("US/Central"));
	        calendar.setTimeInMillis(dateMilisecs);
	        
	        String dateFormat = sdf.format(calendar.getTime());
//	        System.out.println("GregorianCalendar -"+dateFormat);
	        
	        return dateFormat;
	}
	
	public long convertDateToLong (String dateTime){
		long timeMillis = 0;
		
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);
		if (dateTime != null){
			try {
				Date timeDate = sdf.parse(dateTime);
				if (timeDate != null){
					timeMillis = timeDate.getTime();
				}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				System.out.println("Date time: " + dateTime);
				e.printStackTrace();
			}
		}				
		return timeMillis;
	}
	
	public static void main(String[] args){
		DbManager dbUtils = new DbManager();DebugLog.log("-----------main");
//		System.out.println(dbUtils.convertDateToLong("NULL"));
		ScoutInfo scoutBean = new ScoutInfo(50, 501);
		scoutBean.setIpAddress("10.193.6.80");
		scoutBean.setSubmittingTime(new Long("1407840754504"));
		scoutBean.setArrivalTime(new Long("1407840779462"));
		scoutBean.setLastResponseTime(new Long("1407840779524"));
		//System.out.println(dbUtils.convertLongToDate(scoutBean.getArrivalTime()));
	}
}
