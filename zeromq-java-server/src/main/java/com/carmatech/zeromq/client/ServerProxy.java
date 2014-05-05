package com.carmatech.zeromq.client;

import static com.google.common.base.Preconditions.checkNotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.api.IProtocol;
import com.carmatech.zeromq.api.Protocol;
import com.carmatech.zeromq.utilities.IMillisecondsTicker;
import com.carmatech.zeromq.utilities.LocalHost;

/**
 * Class responsible for managing the state of a remote server.
 */
class ServerProxy {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerProxy.class);

	private final String endpoint;
	private final IMillisecondsTicker ticker;
	private final IProtocol protocol;
	private final int pingInterval;
	private final int ttl;

	private boolean isAlive = false;
	private long pingAt; // Send next ping at this time.
	private long timeoutAt; // Expires at this time.

	public ServerProxy(final String endpoint, final int pingInterval, final int ttl, final IMillisecondsTicker ticker) {
		this.endpoint = endpoint;
		this.ticker = checkNotNull(ticker, "Time ticker must NOT be null.");

		this.protocol = new Protocol(LocalHost.HOST_AND_IP);
		this.pingInterval = pingInterval;
		this.ttl = ttl;
		refresh();
	}

	public void refresh() {
		pingAt = nextPing();
		timeoutAt = nextTimeout();
	}

	private long nextPing() {
		return now() + pingInterval;
	}

	private long nextTimeout() {
		return now() + ttl;
	}

	private long now() {
		return ticker.readMillis();
	}

	public boolean isAlive() {
		return isAlive;
	}

	public void enable() {
		if (!isAlive) {
			isAlive = true;
			LOGGER.info("Server [{}] is responsive.", endpoint);
		}
	}

	public void disable() {
		if (isAlive) {
			isAlive = false;
			LOGGER.info("Server [{}] is unresponsive.", endpoint);
		}
	}

	public void pingIfNoRecentActivity(final Socket socket) {
		if (pingAt <= now()) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Pinging server [{}]...", endpoint);
			final ZMsg ping = protocol.ping(endpoint);
			ping.send(socket);
			pingAt = nextPing();
		}
	}

	public long pingAt() {
		return pingAt;
	}

	public long timeoutAt() {
		return timeoutAt;
	}

	public String endpoint() {
		return endpoint;
	}

	public void send(final ZMsg currentRequest, final Socket socket) {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Sending request #[{}] for [{}] to [{}]...", currentRequest.peek(), currentRequest.getLast(), endpoint);

		// Sending destroys the request, so we duplicate the current one:
		final ZMsg request = currentRequest.duplicate();

		request.push(endpoint);
		final boolean success = request.send(socket);
		if (!success) {
			LOGGER.warn("Failed to send request #[{}] for [{}] to [{}]...", currentRequest.peek(), currentRequest.getLast(), endpoint);
		}
	}
}