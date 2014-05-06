package com.carmatech.zeromq.server.pull;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.carmatech.zeromq.api.IProtocol;
import com.carmatech.zeromq.api.Protocol;
import com.carmatech.zeromq.server.IServer;
import com.carmatech.zeromq.utilities.LocalHost;
import com.carmatech.zeromq.utilities.ZeroMQ;
import com.google.common.base.Function;

public abstract class AbstractPullServer implements IServer {

	protected static final int ERROR = -1;

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final IProtocol protocol;
	protected final Function<UUID, byte[]> provider;
	protected final ZContext context;
	protected final Socket server;

	protected final Thread shutdownThread;

	public AbstractPullServer(final int port, final Function<UUID, byte[]> provider) {
		this.provider = checkNotNull(provider);

		shutdownThread = ZeroMQ.addShutdownHook(this);
		context = ZeroMQ.createContext();

		protocol = new Protocol(LocalHost.HOST_AND_IP);
		server = bindTo(port, context);
	}

	private Socket bindTo(final int port, final ZContext context) {
		final Socket server = context.createSocket(ZMQ.ROUTER);

		final String connectEndpoint = "tcp://localhost:" + port; // What clients should use to connect to this server, ...
		server.setIdentity(connectEndpoint.getBytes()); // ...this also serves as the ROUTER socket's identity.

		server.setLinger(0); // Unsent messages are immediately dropped.
		server.setTCPKeepAlive(1); // Keep connections alive.

		final String bindEndpoint = "tcp://*:" + port;
		final int returnCode = server.bind(bindEndpoint);
		if (returnCode == ERROR)
			throw new IllegalStateException("Server failed to bind to [" + bindEndpoint + "].");

		logger.info("Server bound to [{}].", bindEndpoint);
		return server;
	}

	@Override
	public synchronized void close() {
		logger.debug("Closing server...");
		Runtime.getRuntime().removeShutdownHook(shutdownThread);
		beforeClose();
		context.destroy();
		afterClose();
		logger.info("Closed server.");
	}

	/**
	 * Hook called before {@link close}. To override in child classes.
	 */
	protected void beforeClose() {
		// No-op.
	};

	/**
	 * Hook called after {@link close}. To override in child classes.
	 */
	protected void afterClose() {
		// No-op.
	};
}
