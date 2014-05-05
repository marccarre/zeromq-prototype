package com.carmatech.zeromq.client;

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

import com.carmatech.zeromq.server.IServer;
import com.carmatech.zeromq.server.pull.ReqRepServer;
import com.carmatech.zeromq.utilities.LocalHost;
import com.carmatech.zeromq.utilities.Ports;
import com.google.common.testing.FakeTicker;

public class ClientTest {
	private final FakeTicker fakeTicker = new FakeTicker();
	private final Client client = new Client(fakeTicker);

	private final int server1Port = Ports.reserve();
	private final IServer server1 = new ReqRepServer(server1Port, PROVIDER);
	private final Thread server1Thread = new Thread(server1, "server1");

	@Before
	public void setUp() {
		fakeTicker.setAutoIncrementStep(10, TimeUnit.MILLISECONDS);
		server1Thread.start();
	}

	@After
	public void tearDown() {
		closeQuietly(client);
		closeQuietly(server1);
		joinUninterruptibly(server1Thread);
	}

	@Test
	public void sendRequestToResponsiveServerShouldReturnExpectedReply() {
		client.connect("tcp://localhost:" + server1Port);

		UUID uuid = UUID.randomUUID();
		ZMsg reply = client.request(uuid);
		assertThat(reply, is(not(nullValue())));
		assertThat(reply.popString(), is("OK"));
		assertThat(reply.popString(), is(LocalHost.HOST_AND_IP));
		assertThat(reply.popString(), is(uuid.toString()));
		assertThat(reply.popString(), is(toZmqString(uuid)));
	}
}
