package com.carmatech.zeromq.client.pull;

import static com.carmatech.zeromq.api.Command.ERROR;
import static com.carmatech.zeromq.api.Command.OK;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.api.Command;
import com.carmatech.zeromq.utilities.IMillisecondsTicker;
import com.carmatech.zeromq.utilities.MillisecondsTicker;
import com.google.common.base.Ticker;

class Manager {
	/** If not a single service replies within this time, give up. **/
	private static final int REQUEST_TIMEOUT_IN_MILLIS = 10_000;

	/** PING interval for servers we think are alive. **/
	private static final int PING_INTERVAL_IN_MILLIS = 2_000;

	/** Server considered dead if silent for this long. **/
	private static final int SERVER_TTL_IN_MILLIS = 6_000;

	private static final long ONE_MINUTE_IN_MILLIS = TimeUnit.MINUTES.toMillis(1L);

	private static final Logger LOGGER = LoggerFactory.getLogger(Manager.class);

	private final Map<String, ServerProxy> servers = new LinkedHashMap<String, ServerProxy>();
	private final Queue<ServerProxy> activeServers = new ArrayDeque<ServerProxy>();

	private final Queue<Long> timeouts = new PriorityQueue<Long>();

	private long sequenceNumber = 0L; // Number of requests ever sent.
	private ZMsg currentRequest;
	private long currentRequestTimeout;

	private final Socket pipe; // Socket to talk back to application.
	private final Socket router; // Socket to talk to servers.
	private final IMillisecondsTicker ticker;

	public Manager(final Socket pipe, final Socket router, final Ticker ticker) {
		this.pipe = checkNotNull(pipe, "Pipe socket must NOT be null.");
		this.router = checkNotNull(router, "Router socket must NOT be null.");
		this.ticker = new MillisecondsTicker(checkNotNull(ticker, "Time ticker must NOT be null."));
	}

	public long nextTimeout() {
		while (!timeouts.isEmpty()) {
			LOGGER.debug("Timeouts: " + timeouts);
			final long nextTimeout = timeouts.peek();
			final long now = now();
			if (nextTimeout <= now) {
				timeouts.poll();
				continue;
			}
			return nextTimeout - now;
		}
		return ONE_MINUTE_IN_MILLIS;
	}

	private long now() {
		return ticker.readMillis();
	}

	public boolean isProcessingRequest() {
		return currentRequest != null;
	}

	// Callback when we remove server from agent 'servers' hash table
	/**
	 * Processes one message from the frontend application (CONNECT or REQUEST).
	 */
	public void processOutbox() {
		final ZMsg request = ZMsg.recvMsg(pipe);

		final String destination = request.popString();
		final Command command = Command.parse(request.popString());

		switch (command) {
		case REQUEST:
			takeOwnershipOf(request);
			// Do NOT destroy the request.
			break;
		case CONNECT:
			connectTo(destination);
		default:
			request.destroy();
			break;
		}
	}

	private void takeOwnershipOf(final ZMsg request) {
		assert (currentRequest == null); // Strict request-reply cycle.

		currentRequest = request; // Take ownership of request message.
		currentRequest.push(Long.toString(++sequenceNumber)); // Replace "REQUEST" by sequence number.
		// Request expires after global timeout
		currentRequestTimeout = now() + REQUEST_TIMEOUT_IN_MILLIS;
		timeouts.add(currentRequestTimeout);
	}

	private void connectTo(final String endpoint) {
		final ServerProxy server = new ServerProxy(endpoint, PING_INTERVAL_IN_MILLIS, SERVER_TTL_IN_MILLIS, ticker);
		servers.put(endpoint, server);
		activeServers.add(server);
		server.refresh();
		timeouts.add(server.timeoutAt());

		LOGGER.info("Connecting to [{}]...", endpoint);
		router.connect(endpoint);
	}

	/**
	 * Processes one message from a responsive server.
	 */
	public void processInbox() {
		final ZMsg reply = ZMsg.recvMsg(router);

		try {
			final String endpoint = reply.popString();
			final String commandOrSequenceNumber = reply.popString();
			final Command command = Command.parse(commandOrSequenceNumber);

			final ServerProxy server = servers.get(endpoint);
			if (server == null) {
				LOGGER.warn("Received invalid reply from [{}]: server isn't registered.", endpoint);
				return;
			}

			if (!server.isAlive()) {
				activeServers.add(server);
				server.enable();
			}
			timeouts.remove(server.timeoutAt());
			server.refresh();
			timeouts.add(server.timeoutAt());

			doHandleReply(reply, endpoint, commandOrSequenceNumber, command);
		} finally {
			reply.destroy();
		}
	}

	private void doHandleReply(final ZMsg reply, final String endpoint, final String commandOrSequenceNumber, final Command command) {
		switch (command) {
		case PONG:
			break;
		case SEQUENCE_NUMBER:
			if (Integer.parseInt(commandOrSequenceNumber) == sequenceNumber) {
				reply.push(OK.toString());
				reply.send(pipe);
				destroyCurrentRequest();
			} else {
				LOGGER.warn("Received reply [{}] from [{}] too late: currently at [{}].", commandOrSequenceNumber, endpoint, sequenceNumber);
			}
			break;
		default:
			LOGGER.warn("Received invalid reply from [{}]. Command/Sequence: [{}].", endpoint, commandOrSequenceNumber);
			break;
		}
	}

	private void destroyCurrentRequest() {
		currentRequest.destroy();
		currentRequest = null;
	}

	public void destroyCurrentRequestAndReturnFailed() {
		destroyCurrentRequest();
		pipe.send(ERROR.toString());
	}

	public boolean isRequestExpired() {
		return currentRequestTimeout <= now();
	}

	public void sendRequestToFirstActiveServer() {
		final ServerProxy server = firstActiveServer();
		if (server == null)
			return;

		server.send(currentRequest, router);
	}

	/**
	 * Find the first responsive server and return it, or null if none. <br />
	 * Also disable any unresponsive server in the process.
	 * 
	 * @return first responsive server or null if all unresponsive.
	 */
	private ServerProxy firstActiveServer() {
		while (!activeServers.isEmpty()) {
			final ServerProxy server = activeServers.peek();
			if (server.timeoutAt() <= now()) { // Server is unresponsive.
				activeServers.poll();
				server.disable();
			}

			return server;
		}

		return null; // All servers are unresponsive.
	}

	public void pingAllServers() {
		for (final ServerProxy server : servers.values())
			server.pingIfNoRecentActivity(router);
	}
}