package com.carmatech.zeromq.client.pull;

import static com.carmatech.zeromq.utilities.ZeroMQ.ERROR;
import static com.google.common.base.Preconditions.checkNotNull;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZThread.IAttachedRunnable;

import com.google.common.base.Ticker;

/**
 * Pipeline which polls its two sockets: <br/>
 * - for outgoing messages (from pipe), <br/>
 * - for incoming messages (from router).
 */
class Pipeline implements IAttachedRunnable {
	private final Ticker ticker;

	public Pipeline(final Ticker ticker) {
		this.ticker = checkNotNull(ticker, "Time ticker must NOT be null.");
	}

	@Override
	public void run(final Object[] args, final ZContext context, final Socket pipe) {
		final Socket router = context.createSocket(ZMQ.ROUTER);
		final Manager manager = new Manager(pipe, router, ticker);

		final PollItem pipePoller = new PollItem(pipe, ZMQ.Poller.POLLIN);
		final PollItem routerPoller = new PollItem(router, ZMQ.Poller.POLLIN);
		final PollItem[] pollers = { pipePoller, routerPoller };

		while (!Thread.currentThread().isInterrupted()) {
			final long timeout = manager.nextTimeout();
			final int returnCode = ZMQ.poll(pollers, timeout);
			if (returnCode == ERROR)
				break; // ZeroMQ context has been shut down.

			if (pipePoller.isReadable())
				manager.processOutbox();

			if (routerPoller.isReadable())
				manager.processInbox();

			if (manager.isProcessingRequest()) { // Request has not been processed or hasn't received a reply yet.
				if (manager.isRequestExpired()) {
					manager.destroyCurrentRequestAndReturnFailed();
				} else {
					manager.sendRequestToFirstActiveServer();
				}
			}

			manager.pingAllServers();
		}
	}
}