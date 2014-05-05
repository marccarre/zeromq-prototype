package com.carmatech.zeromq.client.pull;

import static com.carmatech.zeromq.api.Command.ERROR;
import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.PROVIDER;
import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.toZmqString;
import static com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMsg;

import com.carmatech.zeromq.client.pull.Client;
import com.carmatech.zeromq.server.IServer;
import com.carmatech.zeromq.server.pull.MultithreadedPullServer;
import com.carmatech.zeromq.server.pull.PullServer;
import com.carmatech.zeromq.utilities.LocalHost;
import com.carmatech.zeromq.utilities.Ports;
import com.google.common.testing.FakeTicker;

public class ClientTest {
	private final FakeTicker fakeTicker = new FakeTicker();
	private final Client client = new Client(fakeTicker);

	private final Integer[] ports = Ports.reserve(2).toArray(new Integer[2]);

	private final int portSimpleServer1 = ports[0];
	private final IServer simpleServer1 = new PullServer(portSimpleServer1, PROVIDER);
	private final Thread threadSimpleServer1 = new Thread(simpleServer1, "simple-server-1");

	private final int portMultithreadedServer1 = ports[1];
	private final IServer multithreadedServer1 = new MultithreadedPullServer(portMultithreadedServer1, PROVIDER);
	private final Thread threadMultithreadedServer1 = new Thread(multithreadedServer1, "multithreaded-server-1");

	@Before
	public void setUp() {
		fakeTicker.setAutoIncrementStep(10, TimeUnit.MILLISECONDS);
	}

	@After
	public void tearDown() {
		closeQuietly(client);
		closeQuietly(simpleServer1);
		joinUninterruptibly(threadSimpleServer1);
		closeQuietly(multithreadedServer1);
		joinUninterruptibly(threadMultithreadedServer1);
	}

	@Test
	public void sendRequestToResponsiveSimpleServerShouldReturnExpectedReply() {
		threadSimpleServer1.start();
		client.connect("tcp://localhost:" + portSimpleServer1);

		UUID uuid = UUID.randomUUID();
		ZMsg reply = client.request(uuid);
		assertThat(reply, is(not(nullValue())));
		assertThat(reply.popString(), is("OK"));
		assertThat(reply.popString(), is(LocalHost.HOST_AND_IP));
		assertThat(reply.popString(), is(uuid.toString()));
		assertThat(reply.popString(), is(toZmqString(uuid)));
	}

	@Test
	public void sendRequestToResponsiveMulithreadedServerShouldReturnExpectedReply() {
		threadMultithreadedServer1.start();
		client.connect("tcp://localhost:" + portMultithreadedServer1);

		UUID uuid = UUID.randomUUID();
		ZMsg reply = client.request(uuid);
		assertThat(reply, is(not(nullValue())));
		assertThat(reply.popString(), is("OK"));
		assertThat(reply.popString(), is(LocalHost.HOST_AND_IP));
		assertThat(reply.popString(), is(uuid.toString()));
		assertThat(reply.popString(), is(toZmqString(uuid)));
	}

	public static void main(final String[] argv) {
		final Client client = new Client();

		// Connect to several endpoints:
		client.connect("tcp://localhost:8887");
		client.connect("tcp://localhost:8888");
		client.connect("tcp://localhost:8889");

		final long start = System.currentTimeMillis();
		for (int i = 0; i < 10; ++i) {
			final UUID uuid = UUID.randomUUID();
			final ZMsg reply = client.request(uuid);

			try {
				final String command = reply.popString();
				System.out.println("Received [" + command + "] reply for [" + uuid + "].");

				if (ERROR.toString().equals(command)) {
					System.out.println("Server unresponsive, aborting...");
					break;
				}
			} finally {
				reply.destroy();
			}
		}
		client.close();
		System.out.println("Average round trip cost: " + (System.currentTimeMillis() - start) + " ms.");
	}
}
