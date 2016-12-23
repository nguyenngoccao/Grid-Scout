package org.kisti.htc.dbmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kisti.htc.scoutmanager.CeInfo;
import org.kisti.htc.scoutmanager.Constants;
import org.kisti.htc.scoutmanager.DebugLog;
import org.kisti.htc.scoutmanager.VoInfo;

public class CeInfoSites {

	public void insertCe(CeInfo ceBean) {
		// TODO Auto-generated method stub
		String sql = "insert into " + Constants.ceTable
				+ " (id, name, voId) values (" + ceBean.getId() + ", \""
				+ ceBean.getName() + "\"" + ", " + ceBean.getVoId() + ")";
		DebugLog.logSql(sql);
		// System.out.println(sql);
		DbManager dbUtils = new DbManager();
		dbUtils.runCommand(sql);
	}

	public void collectCes() {
		DebugLog.log("-----------");
		
		List<String> command = new ArrayList<String>();
		String sql;
		
		
		command.add("lcg-infosites");
		command.add("--vo");
		command.add("biomed");
		command.add("ce");

		ProcessBuilder builder = new ProcessBuilder(command);
		DebugLog.log(command);
		Process p;
		DbManager dbUtils = new DbManager();
		sql = "delete from " + Constants.ceTable;
		dbUtils.runCommand(sql);
		try {
			p = builder.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			String line;
			DebugLog.log(builder.toString());int i=0;
			while ((line = br.readLine()) != null) {DebugLog.log(line);
				Pattern pattern = Pattern
						.compile("(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(.*)");
				Matcher matcher = pattern.matcher(line);
				
				if (matcher.find()) {i++;
					sql = "insert into " + Constants.ceTable
							+ " (id,voId,cpu, free, total, running, waiting,name) values (" + String.valueOf(i)+  
							"," + String.valueOf(1) +
							", " + matcher.group(1).toString() + 
							", " + matcher.group(2).toString() + 
							", " + matcher.group(3).toString() +
							", " + matcher.group(4).toString() +
							", " + matcher.group(5).toString() + 
							", " + "\""+ matcher.group(6).toString()+ "\"" +")";									
					
					dbUtils.runCommand(sql);
					System.out.println(sql);
				}
			}
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CeInfoSites site = new CeInfoSites();
		site.collectCes();
	}

}
