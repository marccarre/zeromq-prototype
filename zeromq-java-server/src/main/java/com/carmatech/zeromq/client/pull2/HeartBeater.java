package com.carmatech.zeromq.client.pull2;

import static com.carmatech.zeromq.api.Command.PONG;
import static com.carmatech.zeromq.utilities.ZeroMQ.ERROR;
import static com.carmatech.zeromq.utilities.ZeroMQ.isSigTerm;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.api.Command;
import com.carmatech.zeromq.api.IProtocol;
import com.carmatech.zeromq.api.Protocol;
import com.carmatech.zeromq.utilities.Duration;
import com.google.common.base.Objects;
import com.google.common.base.Ticker;

public class HeartBeater implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeater.class);

	private final Socket socket;
	private final ConcurrentMap<String, ServerProxy> servers;
	private final Duration pingFrequency;
	private final Ticker ticker;
	private final IProtocol protocol;

	public HeartBeater(final Socket socket, final ConcurrentMap<String, ServerProxy> servers, final Duration pingFrequency, final Ticker ticker) {
		this.socket = checkNotNull(socket, "Socket must NOT be null.");
		this.servers = checkNotNull(servers, "Server repository must NOT be null.");
		this.pingFrequency = checkNotNull(pingFrequency, "Ping frequency must NOT be null.");
		this.ticker = checkNotNull(ticker, "Ticker must NOT be null.");
		protocol = new Protocol(toString());
	}

	public HeartBeater(final Socket socket, final ConcurrentMap<String, ServerProxy> servers, final Duration pingFrequency) {
		this(socket, servers, pingFrequency, Ticker.systemTicker());
	}

	@Override
	public void run() {
		final PollItem poller = new PollItem(socket, ZMQ.Poller.POLLIN);
		final PollItem[] pollers = { poller };

		while (!isInterrupted()) {
			// Check state of each server and deactivate/ping/do nothing:
			for (final ServerProxy server : servers.values()) {
				if (server.shouldBePinged()) {
					if (server.shouldBeDeactivated())
						server.deactivate();
					if (!ping(server))
						return;
				}
			}

			// Keep polling and handling incoming PONG replies for pingFrequency:
			long remainingPollingTimeInNanos = pingFrequency.toNanos();
			final long end = now() + remainingPollingTimeInNanos;

			while (!isInterrupted() && (remainingPollingTimeInNanos > 0)) {

				final long pollingTimeoutInMillis = TimeUnit.NANOSECONDS.toMillis(remainingPollingTimeInNanos);
				if (ZMQ.poll(pollers, pollingTimeoutInMillis) == ERROR) {
					LOGGER.info("Heartbeater has been interrupted: SIGTERM while polling.");
					return;
				}

				// Compute remaining time, which will be positive if we got a reply before the timeout (pingFrequency):
				remainingPollingTimeInNanos = end - now();

				if (poller.isReadable())
					if (!handlePongReply())
						return;
			}
		}

		LOGGER.info("Heartbeater has been interrupted.");
	}

	private boolean isInterrupted() {
		return Thread.currentThread().isInterrupted();
	}

	private boolean ping(final ServerProxy server) {
		final ZMsg ping = protocol.ping(server.endpoint());

		if (ping.send(socket)) {
			LOGGER.debug("Pinged " + server);
			return true;
		} else {
			LOGGER.info("Heartbeater has been interrupted: failed to send PING to {}.", server.endpoint());
			return false;
		}
	}

	private long now() {
		return ticker.read();
	}

	private boolean handlePongReply() {
		try {
			final ZMsg pong = ZMsg.recvMsg(socket);
			if (pong == null) {
				LOGGER.info("Heartbeater has been interrupted: null PONG reply.");
				return false;
			}
			handle(pong);
		} catch (ZMQException e) {
			if (isSigTerm(e)) {
				LOGGER.info("Heartbeater has been interrupted: SIGTERM.");
				return false;
			}
			LOGGER.error("Error [" + e.getErrorCode() + "]: " + e.getMessage(), e);
		}
		return true;
	}

	private void handle(final ZMsg pong) {
		final String endpoint = pong.popString();
		final String commandString = pong.popString();
		final Command command = Command.parse(commandString);

		if (command == PONG) {
			final ServerProxy server = servers.get(endpoint);
			server.activate();
			server.refresh();
		} else {
			LOGGER.warn("Received invalid reply from [{}]: [{}] expected but [{}] received.", endpoint, PONG, commandString);
		}
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("socket", socket.base().typeString()).add("frequency", pingFrequency.toString()).toString();
	}
}
