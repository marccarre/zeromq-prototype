package com.carmatech.zeromq.server.pull;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.zeromq.ZMQ.Error.ETERM;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.api.IProtocol;
import com.google.common.base.Function;

/**
 * Template of Request-Reply runnable: <br />
 * - waits for a request <br />
 * - reply to it (abstract: to implement in child classes) <br />
 * - repeat until: 1) thread is interrupted, 2) ZeroMQ context closes, or 3) socket receives SIGTERM.
 */
abstract class AbstractPullRunnable implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPullRunnable.class);

	protected final Socket socket;
	protected final IProtocol protocol;
	protected final Function<UUID, byte[]> provider;

	AbstractPullRunnable(final Socket socket, final IProtocol protocol, final Function<UUID, byte[]> provider) {
		this.socket = checkNotNull(socket, "Socket must NOT be null.");
		this.protocol = checkNotNull(protocol, "Protocol must NOT be null.");
		this.provider = checkNotNull(provider, "Provider must NOT be null.");
	}

	@Override
	public void run() {
		LOGGER.debug("Hi!");

		while (!isInterrupted()) {
			try {
				final ZMsg request = ZMsg.recvMsg(socket); // Blocking call.
				if (request == null) {
					LOGGER.warn("Server has been interrupted: null request.");
					break;
				}

				reply(request);

			} catch (ZMQException e) {
				if (isSigTerm(e)) {
					LOGGER.warn("Server has been interrupted: SIGTERM.");
					break;
				}

				LOGGER.error("Error [" + e.getErrorCode() + "]: " + e.getMessage(), e);
			}
		}

		if (isInterrupted())
			LOGGER.warn("Server has been interrupted.");

		LOGGER.debug("Bye!");
	}

	private boolean isInterrupted() {
		return Thread.currentThread().isInterrupted();
	}

	private boolean isSigTerm(final ZMQException e) {
		return (e != null) && (e.getErrorCode() == ETERM.getCode());
	}

	protected abstract void reply(final ZMsg request);
}
