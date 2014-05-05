package com.carmatech.zeromq.server.pull;

import java.util.UUID;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.api.IProtocol;
import com.carmatech.zeromq.server.IServer;
import com.google.common.base.Function;

/**
 * Single-threaded request-reply server. <br />
 * Based on "Freelance pattern" (model #3, ROUTER-ROUTER socket): http://rfc.zeromq.org/spec:10
 */
public class PullServer extends AbstractPullServer implements IServer {
	private static final String NAME = PullServer.class.getSimpleName();

	private final Thread serverThread;

	public PullServer(final int port, final Function<UUID, byte[]> provider) {
		super(port, provider);

		// Handle requests in a single background thread:
		serverThread = new Thread(createServerRunnable(server, protocol, provider), NAME + "-handler");
	}

	private AbstractPullRunnable createServerRunnable(final Socket socket, final IProtocol protocol, final Function<UUID, byte[]> provider) {
		return new AbstractPullRunnable(socket, protocol, provider) {
			@Override
			protected void reply(final ZMsg request) {
				final ZMsg reply = protocol.reply(request, provider);
				reply.send(socket);
			}
		};
	}

	@Override
	public void run() {
		logger.debug("Starting server...");
		serverThread.start();
		logger.info("Server is now ready to serve incoming requests.");
	}
}