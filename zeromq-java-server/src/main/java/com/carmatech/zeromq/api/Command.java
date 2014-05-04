package com.carmatech.zeromq.api;

import org.apache.commons.lang3.StringUtils;

public enum Command {
	CONNECT, PING, PONG, REQUEST, SEQUENCE_NUMBER, OK, ERROR, UNKNOWN;

	public static Command parse(final String string) {
		if (string == null || string.isEmpty())
			return UNKNOWN;

		if (StringUtils.isNumeric(string))
			return SEQUENCE_NUMBER;

		return tryParse(string);
	}

	private static Command tryParse(final String string) {
		try {
			return valueOf(string);
		} catch (Exception e) {
			return UNKNOWN;
		}
	}
}
