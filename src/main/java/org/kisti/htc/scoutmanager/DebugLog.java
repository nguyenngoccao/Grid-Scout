package org.kisti.htc.scoutmanager;

import java.util.List;


public class DebugLog {
	public final static boolean DEBUG = true;
	public final static boolean DEBUG_MAIN = true;
	public final static boolean DEBUG_THREAD = true;
	public final static boolean DEBUG_SQL = true;
	public final static boolean DEBUG_CE = true;
	public final static boolean DEBUG_SCOUT = true;
	public final static boolean DEBUG_FROMSCOUT = true;
	public final static boolean DEBUG_VO = true;
	public final static boolean DEBUG_CESTATISTIC = true;

	public static void log(String message) {
		if (DEBUG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logMain() {
		if (DEBUG_MAIN) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber);
		}
	}
	public static void log(List<String> message) {
		if (DEBUG) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logMain(String message) {
		if (DEBUG_MAIN) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logThread(String message) {
		if (DEBUG_THREAD) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logThread() {
		if (DEBUG_THREAD) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber);
		}
	}

	public static void logSql(String message) {
		if (DEBUG_SQL) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + "sql ==========:"+ message);
		}
	}
	public static void logCe(String message) {
		if (DEBUG_CE) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logVo(String message) {
		if (DEBUG_VO) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logCestatistic(String message) {
		if (DEBUG_CESTATISTIC) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logScout(String message) {
		if (DEBUG_SCOUT) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + message);
		}
	}
	public static void logFromScout(String message) {
		if (DEBUG_FROMSCOUT) {
			String fullClassName = Thread.currentThread().getStackTrace()[2]
					.getClassName();
			String className = fullClassName.substring(fullClassName
					.lastIndexOf(".") + 1);
			String methodName = Thread.currentThread().getStackTrace()[2]
					.getMethodName();
			int lineNumber = Thread.currentThread().getStackTrace()[2]
					.getLineNumber();

			System.out.println(className + "." + methodName + "():" + lineNumber + "Sent from Scout" + message);
		}
	}

}
