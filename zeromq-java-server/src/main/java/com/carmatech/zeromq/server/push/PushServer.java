package com.carmatech.zeromq.server.push;

import static com.carmatech.zeromq.utilities.ZeroMQ.bindTo;
import static com.carmatech.zeromq.utilities.ZeroMQ.isSigTerm;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.server.IServer;
import com.carmatech.zeromq.utilities.LocalHost;
import com.carmatech.zeromq.utilities.ZeroMQ;

public class PushServer implements IServer {
	private static final UUID POISON_PILL = UUID.fromString("00000000-DEAD-DEAD-DEAD-000000000000");
	private static final String NAME = PushServer.class.getSimpleName();

	private static final Logger LOGGER = LoggerFactory.getLogger(PushServer.class);

	private final BlockingQueue<Pair<UUID, byte[]>> queue;
	private final ZContext context;
	private final Thread shutdownThread;
	private final Thread publisherThread;

	public PushServer(final int port, final BlockingQueue<Pair<UUID, byte[]>> queue) {
		this.queue = checkNotNull(queue, "Supplying queue must NOT be null.");

		shutdownThread = ZeroMQ.addShutdownHook(this);

		context = ZeroMQ.createContext();

		final Socket publisher = bindServerTo(port, context);
		publisherThread = new Thread(createServerRunnable(publisher, queue), NAME + "-publisher");
	}

	private Socket bindServerTo(final int port, final ZContext context) {
		final Socket server = context.createSocket(ZMQ.PUB);

		server.setLinger(0); // Unsent messages are immediately dropped.
		server.setTCPKeepAlive(1); // Keep connections alive.

		final String bindEndpoint = "tcp://*:" + port;
		bindTo(bindEndpoint, server);

		LOGGER.info("Server bound to [{}].", bindEndpoint);
		return server;
	}

	private Runnable createServerRunnable(final Socket socket, final BlockingQueue<Pair<UUID, byte[]>> supplier) {
		return new Runnable() {
			@Override
			public void run() {
				LOGGER.debug("Hi!");

				while (!isInterrupted()) {
					try {
						final Pair<UUID, byte[]> item = supplier.take(); // Blocking call.
						if (isPoisonPill(item)) {
							LOGGER.info("'Poison pill' received, now stopping server...");
							break;
						}

						final ZMsg message = new ZMsg();
						message.add(item.getKey().toString());
						message.add(LocalHost.HOST_AND_IP);
						message.add(item.getValue());
						message.send(socket);
						LOGGER.debug("Server published: [{}].", item);

					} catch (ZMQException e) {
						if (isSigTerm(e)) {
							LOGGER.warn("Server has been interrupted: SIGTERM.");
							break;
						}
						LOGGER.error("Error [" + e.getErrorCode() + "]: " + e.getMessage(), e);
					} catch (InterruptedException e) {
						LOGGER.warn("Server has been interrupted: " + e.getMessage());
						break;
					}
				}

				if (isInterrupted())
					LOGGER.warn("Server has been interrupted.");

				LOGGER.debug("Bye!");
			}

			private boolean isInterrupted() {
				return Thread.currentThread().isInterrupted();
			}

			private boolean isPoisonPill(final Pair<UUID, byte[]> item) {
				return POISON_PILL.equals(item.getKey());
			}
		};
	}

	@Override
	public void run() {
		LOGGER.debug("Starting server...");
		publisherThread.start();
		LOGGER.info("Server is now ready to publish incoming items.");
	}

	@Override
	public synchronized void close() {
		LOGGER.debug("Closing server...");
		if (!Thread.currentThread().equals(shutdownThread))
			Runtime.getRuntime().removeShutdownHook(shutdownThread);
		queue.add(ImmutablePair.of(POISON_PILL, (byte[]) null));
		context.destroy();
		LOGGER.info("Closed server.");
	}
}
