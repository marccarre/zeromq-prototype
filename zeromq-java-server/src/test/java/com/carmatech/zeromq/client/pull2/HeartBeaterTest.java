package com.carmatech.zeromq.client.pull2;

import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.PROVIDER;
import static com.carmatech.zeromq.utilities.matchers.PingCommandMatcher.isPingCommandWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.api.IProtocol;
import com.carmatech.zeromq.api.Protocol;
import com.carmatech.zeromq.utilities.Duration;
import com.google.common.util.concurrent.Uninterruptibles;

public class HeartBeaterTest {
	private static final AtomicLong THREAD_COUNTER = new AtomicLong(0L);
	private static final ZContext CONTEXT = new ZContext();
	private static final String SERVER_ENDPOINT = "tcp://192.168.1.1:1337";

	private final Socket dispatcherSocket = CONTEXT.createSocket(ZMQ.PAIR);
	private final Socket heartBeaterSocket = CONTEXT.createSocket(ZMQ.PAIR);
	private final String heartBeaterEndpoint = "inproc://heartbeater-pipe-" + heartBeaterSocket.hashCode();
	private final IProtocol serverProtocol = new Protocol(SERVER_ENDPOINT);
	private final Duration pingFrequency = new Duration(50, TimeUnit.MILLISECONDS);

	private final ConcurrentMap<String, ServerProxy> serverRepository = new ConcurrentHashMap<String, ServerProxy>();
	private final HeartBeater heartBeater = new HeartBeater(heartBeaterSocket, serverRepository, pingFrequency);

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Before
	public void setUp() {
		heartBeaterSocket.bind(heartBeaterEndpoint);
		dispatcherSocket.connect(heartBeaterEndpoint);
	}

	@AfterClass
	public static void tearDownFixture() {
		CONTEXT.destroy();
	}

	@Test
	public void initialPingOnAResponsiveServerShouldActivateIt() {
		// Setup and preconditions:
		Thread heartBeaterThread = new Thread(heartBeater, "heartbeater-" + THREAD_COUNTER.getAndIncrement());
		ServerProxy server = new ServerProxy(SERVER_ENDPOINT, pingFrequency);
		serverRepository.put(SERVER_ENDPOINT, server);
		assertThat(server.isActive(), is(false));

		heartBeaterThread.start();

		ZMsg ping = ZMsg.recvMsg(dispatcherSocket);
		assertThat(ping, isPingCommandWith(SERVER_ENDPOINT, heartBeater.toString()));

		// Simulate reply from the server, as it is supposed to be "active":
		long pingAtBeforeReply = server.pingAt();
		serverProtocol.reply(ping, PROVIDER).send(dispatcherSocket);

		// Wait for heartbeater to react to the PONG reply:
		do {
			Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
		} while (pingAtBeforeReply == server.pingAt());

		assertThat(server.isActive(), is(true));
		assertThat(server.pingAt(), greaterThan(pingAtBeforeReply + pingFrequency.toNanos()));
		assertThat(server.pingAt(), lessThan(pingAtBeforeReply + (2 * pingFrequency.toNanos())));
	}

	@Test
	public void pingOnAServerWhichBecameUnresponsiveShouldDesactivateIt() {
		// Setup and preconditions:
		Thread heartBeaterThread = new Thread(heartBeater, "heartbeater-" + THREAD_COUNTER.getAndIncrement());
		ServerProxy server = new ServerProxy(SERVER_ENDPOINT, pingFrequency);
		serverRepository.put(SERVER_ENDPOINT, server);
		server.activate();
		long pingAtBeforeReply = server.pingAt();
		assertThat(server.isActive(), is(true));

		heartBeaterThread.start();

		int count = 0;
		do {
			ZMsg.recvMsg(dispatcherSocket);
			++count;
		} while (server.isActive());

		assertThat(server.isActive(), is(false)); // Server has now been detected as unresponsive.
		assertThat(count, is(2)); // 1 initial ping + 1 subsequent ping.
		assertThat(server.pingAt(), lessThan(pingAtBeforeReply + ((count + 1) * pingFrequency.toMillis())));
	}

	@Test
	public void pingOnAResponsiveServerShouldKeepItActivated() {
		// Setup and preconditions:
		Thread heartBeaterThread = new Thread(heartBeater, "heartbeater-" + THREAD_COUNTER.getAndIncrement());
		ServerProxy server = new ServerProxy(SERVER_ENDPOINT, pingFrequency);
		serverRepository.put(SERVER_ENDPOINT, server);
		server.activate();
		assertThat(server.isActive(), is(true));

		heartBeaterThread.start();

		ZMsg ping = ZMsg.recvMsg(dispatcherSocket);
		assertThat(ping, isPingCommandWith(SERVER_ENDPOINT, heartBeater.toString()));

		// Simulate reply from the server, as it is supposed to be "active":
		long pingAtBeforeReply = server.pingAt();
		serverProtocol.reply(ping, PROVIDER).send(dispatcherSocket);

		// Wait for heartbeater to react to the PONG reply:
		do {
			Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
		} while (pingAtBeforeReply == server.pingAt());

		assertThat(server.isActive(), is(true));
		assertThat(server.pingAt(), greaterThan(pingAtBeforeReply + pingFrequency.toNanos()));
		assertThat(server.pingAt(), lessThan(pingAtBeforeReply + (2 * pingFrequency.toNanos())));
	}

	@Test
	public void replyingWithAnythingElseThanAPongDoesNotHaveAnyEffect() {
		// Setup and preconditions:
		Thread heartBeaterThread = new Thread(heartBeater, "heartbeater-" + THREAD_COUNTER.getAndIncrement());
		ServerProxy server = new ServerProxy(SERVER_ENDPOINT, pingFrequency);
		serverRepository.put(SERVER_ENDPOINT, server);
		assertThat(server.isActive(), is(false));

		heartBeaterThread.start();

		ZMsg ping = ZMsg.recvMsg(dispatcherSocket);
		assertThat(ping, isPingCommandWith(SERVER_ENDPOINT, heartBeater.toString()));

		// Simulate reply from the server, as it is supposed to be "active", but send a PING instead of a PONG:
		long pingAtBeforeReply = server.pingAt();
		serverProtocol.ping(SERVER_ENDPOINT).send(dispatcherSocket);

		// Wait for heartbeater to react to the invalid PING reply and send another "PING:
		ZMsg.recvMsg(dispatcherSocket);

		assertThat(server.isActive(), is(false));
		assertThat(server.pingAt(), is(pingAtBeforeReply));
	}

	@Test
	public void createHeartBeaterWithNullSocketShouldThrowNullPointerException() {
		exception.expect(NullPointerException.class);
		exception.expectMessage("Socket must NOT be null.");
		new HeartBeater(null, serverRepository, pingFrequency);
	}

	@Test
	public void createHeartBeaterWithNullServerRepositoryShouldThrowNullPointerException() {
		exception.expect(NullPointerException.class);
		exception.expectMessage("Server repository must NOT be null.");
		new HeartBeater(heartBeaterSocket, null, pingFrequency);
	}

	@Test
	public void createHeartBeaterWithNullPingFrequencyShouldThrowNullPointerException() {
		exception.expect(NullPointerException.class);
		exception.expectMessage("Ping frequency must NOT be null.");
		new HeartBeater(heartBeaterSocket, serverRepository, null);
	}
}
