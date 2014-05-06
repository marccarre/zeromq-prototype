package com.carmatech.zeromq.client.push;

import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.PROVIDER;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carmatech.zeromq.server.push.PushServer;
import com.carmatech.zeromq.utilities.Ports;
import com.google.common.collect.Queues;

public class PushClientTest {
	final int port = Ports.reserve();
	final BlockingQueue<Pair<UUID, byte[]>> queue = Queues.newArrayBlockingQueue(16);
	final PushServer server = new PushServer(port, queue);
	final PushClient client = new PushClient();

	@Before
	public void setUp() {
		server.run();
	}

	@After
	public void tearDown() {
		client.close();
		server.close();
	}

	@Test
	public void consumeOneMessageFromOneUUID() {
		client.connect("tcp://localhost:" + port);
		UUID uuid1 = UUID.fromString("7af76005-d4e2-11e3-9714-3c15c2baa558");
		UUID uuid2 = UUID.fromString("7b68f233-d4e2-11e3-94d0-3c15c2baa558");
		UUID uuid3 = UUID.fromString("7bad73dc-d4e2-11e3-8709-3c15c2baa558");
		client.filter(uuid2);

		queue.add(ImmutablePair.of(uuid1, PROVIDER.apply(uuid1)));
		queue.add(ImmutablePair.of(uuid2, PROVIDER.apply(uuid2)));
		queue.add(ImmutablePair.of(uuid3, PROVIDER.apply(uuid3)));

		Pair<UUID, byte[]> pair2 = client.receive();
		assertThat(pair2, is(not(nullValue())));
		assertThat(pair2.getKey(), is(uuid2));
		assertThat(pair2.getValue(), is(PROVIDER.apply(uuid2)));

		assertThat(client.numReceivedMessages(), is(1L));

	}

	@Test
	public void consumeMessagesFromMultipleUUIDs() {
		client.connect("tcp://localhost:" + port);
		UUID uuid1 = UUID.fromString("7af76005-d4e2-11e3-9714-3c15c2baa558");
		UUID uuid2 = UUID.fromString("7b68f233-d4e2-11e3-94d0-3c15c2baa558");
		UUID uuid3 = UUID.fromString("7bad73dc-d4e2-11e3-8709-3c15c2baa558");
		client.filter(uuid1);
		client.filter(uuid2);
		client.filter(uuid3);

		queue.add(ImmutablePair.of(uuid1, PROVIDER.apply(uuid1)));
		queue.add(ImmutablePair.of(uuid2, PROVIDER.apply(uuid2)));
		queue.add(ImmutablePair.of(uuid3, PROVIDER.apply(uuid3)));

		Pair<UUID, byte[]> pair1 = client.receive();
		assertThat(pair1, is(not(nullValue())));
		assertThat(pair1.getKey(), is(uuid1));
		assertThat(pair1.getValue(), is(PROVIDER.apply(uuid1)));

		Pair<UUID, byte[]> pair2 = client.receive();
		assertThat(pair2, is(not(nullValue())));
		assertThat(pair2.getKey(), is(uuid2));
		assertThat(pair2.getValue(), is(PROVIDER.apply(uuid2)));

		Pair<UUID, byte[]> pair3 = client.receive();
		assertThat(pair3, is(not(nullValue())));
		assertThat(pair3.getKey(), is(uuid3));
		assertThat(pair3.getValue(), is(PROVIDER.apply(uuid3)));

		assertThat(client.numReceivedMessages(), is(3L));
	}
}
