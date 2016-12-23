package org.kisti.htc.dbmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kisti.htc.scoutmanager.CeInfo;
import org.kisti.htc.scoutmanager.DebugLog;

public class InitCeInfo {
	
	public void initCeInfoInDb() {
		DebugLog.log("-----------");
		
		List<String> command = new ArrayList<String>();
		
		CeInfo ceInfo;
		int id;
		DbCe dbCe = new DbCe();
		command.add("lcg-infosites");
		command.add("--vo");
		command.add("biomed");
		command.add("ce");

		ProcessBuilder builder = new ProcessBuilder(command);
		DebugLog.log(command);
		Process p;
		
		try {
			p = builder.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			String line;
			DebugLog.log(builder.toString());
			id = 0;
			while ((line = br.readLine()) != null) {DebugLog.log(line);
				Pattern pattern = Pattern
						.compile("(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(.*)");
				Matcher matcher = pattern.matcher(line);
				
				if (matcher.find()) {
					
					ceInfo = new CeInfo(id, matcher.group(6).toString(), 1);
					
					ceInfo.setCpuNum(Integer.parseInt(matcher.group(1)));
					ceInfo.setFreeNum(Integer.parseInt(matcher.group(2)));
					ceInfo.setTotalNum(Integer.parseInt(matcher.group(3)));
					ceInfo.setRunningNum(Integer.parseInt(matcher.group(4)));
					ceInfo.setWaitingNum(Integer.parseInt(matcher.group(5)));
					
					dbCe.initCeInfoInDb(ceInfo);
					id ++;
				}
			}
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		InitCeInfo ceInfo = new InitCeInfo();
		ceInfo.initCeInfoInDb();
	}

}
