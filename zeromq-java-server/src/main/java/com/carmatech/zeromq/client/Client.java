package com.carmatech.zeromq.client;

import static com.carmatech.zeromq.api.Command.ERROR;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.util.UUID;

import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZThread;

import com.carmatech.zeromq.api.IProtocol;
import com.carmatech.zeromq.api.Protocol;
import com.carmatech.zeromq.utilities.LocalHost;
import com.google.common.base.Ticker;

/**
 * Resilient, broker-less, request-reply client. <br />
 * Based on "Freelance pattern": http://rfc.zeromq.org/spec:10
 */
public class Client implements Closeable {
	private static final String UNUSED = "UNUSED";

	private final ZContext context;
	private final Socket pipe;
	private final IProtocol protocol;

	public Client() {
		this(new ZContext(), Ticker.systemTicker());
	}

	public Client(final ZContext context, final Ticker ticker) {
		this.context = checkNotNull(context, "ZeroMQ context must NOT be null.");
		pipe = ZThread.fork(context, new Pipeline(ticker));
		protocol = new Protocol(LocalHost.HOST_AND_IP);
	}

	/**
	 * Connect to the specified server. <br />
	 * WARNING: Waits 100 milliseconds for the connection to come up, in order to avoid sending all requests to a single server, in case of multiple servers
	 * being configured.
	 * 
	 * @param endpoint
	 *            Endpoint of the server to connect to.
	 */
	public void connect(final String endpoint) {
		final ZMsg connect = protocol.connect(endpoint);
		connect.send(pipe);
		waitForConnectionToBeReady();
	}

	private void waitForConnectionToBeReady() {
		try {
			Thread.sleep(100L);
		} catch (InterruptedException e) {
		}
	}

	public ZMsg request(final UUID uuid) {
		final ZMsg request = protocol.request(uuid, UNUSED);
		request.send(pipe);
		return ZMsg.recvMsg(pipe);
	}

	@Override
	public void close() {
		context.destroy();
	}

	public static void main(final String[] argv) {
		final Client client = new Client();

		// Connect to several endpoints:
		client.connect("tcp://localhost:8887");
		client.connect("tcp://localhost:8888");
		client.connect("tcp://localhost:8889");

		final long start = System.currentTimeMillis();
		for (int i = 0; i < 10; ++i) {
			final UUID uuid = UUID.randomUUID();
			final ZMsg reply = client.request(uuid);

			try {
				final String command = reply.popString();
				System.out.println("Received [" + command + "] reply for [" + uuid + "].");

				if (ERROR.toString().equals(command)) {
					System.out.println("Server unresponsive, aborting...");
					break;
				}
			} finally {
				reply.destroy();
			}
		}
		client.close();
		System.out.println("Average round trip cost: " + (System.currentTimeMillis() - start) + " ms.");
	}
}