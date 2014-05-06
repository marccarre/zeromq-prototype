package com.carmatech.zeromq.utilities;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import com.carmatech.zeromq.server.IServer;

public final class ZeroMQ {
	private static final Logger LOGGER = LoggerFactory.getLogger(ZeroMQ.class);
	private static final int NUM_IO_THREADS = 1;
	private static final int HIGH_WATER_MARK = 1000;

	private ZeroMQ() {
		// Pure utility class, do NOT instantiate.
	}

	public static ZContext createContext() {
		final ZContext context = new ZContext(NUM_IO_THREADS);

		if (context.getHWM() < HIGH_WATER_MARK) {
			context.setHWM(HIGH_WATER_MARK);
			LOGGER.info("High-water mark set to [{}].", HIGH_WATER_MARK);
		}

		return context;
	}

	public static Thread addShutdownHook(final IServer server) {
		final Thread shutdownThread = new Thread(server.getClass() + "-shutdown-hook") {
			@Override
			public void run() {
				LOGGER.warn("Interrupt received or shutdown hook triggered, now stopping server [{}]...", server);
				IOUtils.closeQuietly(server);
			}
		};
		Runtime.getRuntime().addShutdownHook(shutdownThread);
		return shutdownThread;
	}
}
