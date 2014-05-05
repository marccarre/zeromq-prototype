package com.carmatech.zeromq.utilities;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.zeromq.ZMQ;

import com.google.common.base.Function;

public final class ProtocolPayloadUtilities {
	private ProtocolPayloadUtilities() {
		// Pure testing utility class, do NOT instantiate.
	}

	public static final Function<UUID, byte[]> PROVIDER = new Function<UUID, byte[]>() {
		@Override
		public byte[] apply(final UUID uuid) {
			return toBytes(uuid);
		}
	};

	public static String toZmqString(UUID uuid) {
		return toString(toBytes(uuid)).toUpperCase();
	}

	private static byte[] toBytes(final UUID uuid) {
		return ByteBuffer.allocate(Integer.SIZE).putInt(uuid.hashCode()).array();
	}

	private static String toString(final byte[] data) {
		return containsSpecialChars(data) ? new String(data, ZMQ.CHARSET) : toHexString(data);
	}

	private static boolean containsSpecialChars(final byte[] data) {
		for (int i = 0; i < data.length; i++)
			if (data[i] < 32 || data[i] > 127)
				return false;
		return true;
	}

	private static final String HEXADECIMALS = "0123456789ABCDEF";

	public static String toHexString(final byte[] data) {
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			int first = data[i] >>> 4 & 0xf;
			int second = data[i] & 0xf;
			builder.append(HEXADECIMALS.charAt(first));
			builder.append(HEXADECIMALS.charAt(second));
		}
		return builder.toString();
	}
}
