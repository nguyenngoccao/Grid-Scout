package org.kisti.htc.scoutmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*+-------+-------------+------+-----+---------+-------+
| Field | Type        | Null | Key | Default | Extra |
+-------+-------------+------+-----+---------+-------+
| id    | int(11)     | NO   | PRI | 0       |       |
| name  | varchar(50) | YES  |     | NULL    |       |
+-------+-------------+------+-----+---------+-------+*/

public class VoInfo {

	private int id;
	private String name;
	
	public VoInfo(int id, String name) {
		// TODO Auto-generated constructor stub
		setId(id);
		setName(name);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name){
		DebugLog.log("vo name ======== " + name);
		this.name = name;
	}
	
	public VoInfo(String name){
		setName(name);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
}
