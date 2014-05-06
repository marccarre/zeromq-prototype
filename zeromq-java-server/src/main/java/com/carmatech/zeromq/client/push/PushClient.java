package com.carmatech.zeromq.client.push;

import static com.carmatech.zeromq.utilities.ZeroMQ.createContext;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.google.common.util.concurrent.Uninterruptibles;

public class PushClient implements Closeable {
	private static final Logger LOGGER = LoggerFactory.getLogger(PushClient.class);
	private final ZContext context;
	private final Socket socket;
	private final AtomicLong numReceivedMessages = new AtomicLong(0L);

	public PushClient() {
		context = createContext();
		socket = context.createSocket(ZMQ.SUB);
	}

	/**
	 * Connect to the specified server. <br />
	 * WARNING: Waits 100 milliseconds for the connection to come up, in order to avoid loosing messages while connecting.
	 * 
	 * @param endpoint
	 *            Endpoint of the server to connect to.
	 */
	public void connect(final String endpoint) {
		socket.connect(endpoint);
		Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
		LOGGER.info("PUSH client now connected to [{}].", endpoint);
	}

	public void subscribeTo(final UUID uuid) {
		socket.subscribe(uuid.toString().getBytes());
		LOGGER.info("PUSH client now accepting data for [{}].", uuid);
	}

	public Pair<UUID, byte[]> receive() {
		final ZMsg message = ZMsg.recvMsg(socket);
		final long sequenceId = numReceivedMessages.incrementAndGet();
		final UUID uuid = UUID.fromString(message.popString());
		final String source = message.popString();
		final byte[] data = message.pop().getData();
		LOGGER.debug("Received message #{} with key [{}] from [{}].", sequenceId, uuid, source);
		return ImmutablePair.of(uuid, data);
	}

	public long numReceivedMessages() {
		return numReceivedMessages.get();
	}

	@Override
	public synchronized void close() {
		context.destroy();
	}
}
