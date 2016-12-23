package org.kisti.htc.scoutmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kisti.htc.dbmanager.DbCe;
import org.kisti.htc.dbmanager.DbCe;
import org.kisti.htc.dbmanager.DbVo;
import org.kisti.htc.dbmanager.DbVo;

public class GliteController {

	public void collectCes (){DebugLog.log("-----------");
		DbVo dbVo = new DbVo();
		DbCe dbCe = new DbCe();
		List<VoInfo> vos = dbVo.collectVos();
//		List <CeBean> ceBeans = new ArrayList<CeBean>();
		int ceId = 0;
		for (VoInfo vo: vos){
			ceId-=2;
			List<String> command = new ArrayList<String>();
	        command.add("lcg-infosites");
	        command.add("--vo");
	        command.add(vo.getName());
	        command.add("ce");

	        ProcessBuilder builder = new ProcessBuilder(command); DebugLog.log(command);       
			Process p;
			try {
				p = builder.start();
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line;
				while ((line = br.readLine()) != null) {
					Pattern pattern = Pattern.compile("(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(.*)");
					Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						String ceName = matcher.group(6);
						DebugLog.log("CE: " + ceName);
						CeInfo ceBean = new CeInfo(ceId, ceName, vo.getId());
//						ceBeans.add(ceBean);
						dbCe.insertCeToDb(ceBean);
					}
					ceId++;
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	public static void main (String[] args){
		GliteController gliteResource = new GliteController();
		gliteResource.collectCes();
	}
}
