package com.carmatech.zeromq.server.pull;

import java.io.IOException;
import java.nio.ByteBuffer;
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
public class ReqRepServer extends AbstractReqRepServer implements IServer {
	private static final String NAME = ReqRepServer.class.getSimpleName();

	private final Thread serverThread;

	public ReqRepServer(final int port, final Function<UUID, byte[]> provider) {
		super(port, provider);

		// Handle requests in a single background thread:
		serverThread = new Thread(createServerRunnable(server, protocol, provider), NAME + "-handler");
	}

	private AbstractReqRepRunnable createServerRunnable(final Socket socket, final IProtocol protocol, final Function<UUID, byte[]> provider) {
		return new AbstractReqRepRunnable(socket, protocol, provider) {
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

	public static void main(final String[] argv) throws IOException {
		final IServer server = new ReqRepServer(8888, new Function<UUID, byte[]>() {
			@Override
			public byte[] apply(final UUID uuid) {
				return ByteBuffer.allocate(Integer.SIZE).putInt(uuid.hashCode()).array();
			}
		});
		server.run();

		System.out.println("Press <Enter> to exit.");
		System.in.read();

		server.close();
		System.out.println("Bye!");
	}
}