package com.carmatech.zeromq.client.pull;

import static com.carmatech.zeromq.utilities.ZeroMQ.createContext;

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
		this(Ticker.systemTicker());
	}

	public Client(final Ticker ticker) {
		context = createContext();
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
}