package com.carmatech.zeromq.utilities;

import static org.zeromq.ZMQ.Error.ETERM;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import com.carmatech.zeromq.server.IServer;

public final class ZeroMQ {
	private static final Logger LOGGER = LoggerFactory.getLogger(ZeroMQ.class);

	private static final int NUM_IO_THREADS = 1;
	private static final int HIGH_WATER_MARK = 1000;
	public static final int ERROR = -1;

	private ZeroMQ() {
		// Pure utility class, do NOT instantiate.
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

	public static ZContext createContext() {
		final ZContext context = new ZContext(NUM_IO_THREADS);

		if (context.getHWM() < HIGH_WATER_MARK) {
			context.setHWM(HIGH_WATER_MARK);
			LOGGER.info("High-water mark set to [{}].", HIGH_WATER_MARK);
		}

		return context;
	}

	public static void bindTo(final String endpoint, final Socket socket) {
		final int returnCode = socket.bind(endpoint);
		if (returnCode == ERROR)
			throw new IllegalStateException("Server failed to bind to [" + endpoint + "].");
	}

	public static boolean isSigTerm(final ZMQException e) {
		return (e != null) && (e.getErrorCode() == ETERM.getCode());
	}
}
