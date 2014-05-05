package com.carmatech.zeromq.server.pull;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.api.IProtocol;
import com.carmatech.zeromq.server.IServer;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Multi-threaded request-reply server. <br />
 * Based on "Freelance pattern" (model #3, ROUTER-ROUTER socket): http://rfc.zeromq.org/spec:10
 */
public class MultithreadedReqRepServer extends AbstractReqRepServer implements IServer {
	private static final String NAME = MultithreadedReqRepServer.class.getSimpleName();
	private static final String INPROC_ENDPOINT = "inproc://" + NAME + "-work-queue";

	private final Socket dispatcher;
	private final Thread dispatcherThread;

	private final int degreeOfParallelism;
	private final ExecutorService workersPool;

	public MultithreadedReqRepServer(final int port, final Function<UUID, byte[]> provider) {
		this(port, provider, Runtime.getRuntime().availableProcessors());
	}

	public MultithreadedReqRepServer(final int port, final Function<UUID, byte[]> provider, final int degreeOfParallelism) {
		super(port, provider);

		dispatcher = bindDispatcherTo(INPROC_ENDPOINT, context);
		dispatcherThread = new Thread(createDispatcherRunnable(server, dispatcher), NAME + "-queue");

		this.degreeOfParallelism = degreeOfParallelism;
		workersPool = Executors.newFixedThreadPool(degreeOfParallelism, new ThreadFactoryBuilder().setNameFormat(NAME + "-worker-%d").build());
	}

	private Socket bindDispatcherTo(final String endpoint, final ZContext context) {
		final Socket dispatcher = context.createSocket(ZMQ.DEALER);

		dispatcher.setLinger(0); // Unsent messages are immediately dropped.
		dispatcher.setTCPKeepAlive(1); // Keep connections alive.

		final int returnCode = dispatcher.bind(endpoint);
		if (returnCode == ERROR)
			throw new IllegalStateException("Dispatcher failed to bind to [" + endpoint + "].");

		logger.info("Dispatcher bound to [{}].", endpoint);
		return dispatcher;
	}

	private Runnable createDispatcherRunnable(final Socket server, final Socket dispatcher) {
		return new Runnable() {
			@Override
			public void run() {
				logger.debug("Queue started.");
				ZMQ.proxy(server, dispatcher, null);
				logger.debug("Queue stopped.");
			}
		};
	}

	@Override
	public synchronized void run() {
		logger.debug("Starting server...");

		for (int i = 0; i < degreeOfParallelism; ++i)
			workersPool.submit(createWorkerRunnable(createWorkerSocket(), protocol, provider));
		workersPool.shutdown();

		dispatcherThread.start();

		logger.info("Server is now ready to serve incoming requests.");
	}

	private Socket createWorkerSocket() {
		final Socket socket = context.createSocket(ZMQ.ROUTER);
		socket.setLinger(0);
		socket.connect(INPROC_ENDPOINT);
		return socket;
	}

	private AbstractReqRepRunnable createWorkerRunnable(final Socket socket, final IProtocol protocol, final Function<UUID, byte[]> provider) {
		return new AbstractReqRepRunnable(socket, protocol, provider) {
			@Override
			protected void reply(final ZMsg request) {
				final ZFrame workerId = request.pop();
				final ZMsg reply = protocol.reply(request, provider);
				reply.push(workerId);
				reply.send(socket);
			}
		};
	}
}