package com.carmatech.zeromq.utilities;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class LocalHost {
	public static final String NOT_AVAILABLE = "N/A";

	private LocalHost() {
		// Pure constant placeholder, do NOT instantiate.
	}

	public static String IP = getIpAddress();
	public static String HOST_AND_IP = getLocalHost();

	private static String getIpAddress() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return NOT_AVAILABLE + ": " + e.getMessage();
		}
	}

	private static String getLocalHost() {
		try {
			return InetAddress.getLocalHost().toString();
		} catch (UnknownHostException e) {
			return NOT_AVAILABLE + ": " + e.getMessage();
		}
	}
}
