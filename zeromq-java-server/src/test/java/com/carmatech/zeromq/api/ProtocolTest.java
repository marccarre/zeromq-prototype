package com.carmatech.zeromq.api;

import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.PROVIDER;
import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.toZmqString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.zeromq.ZMsg;

public class ProtocolTest {
	private static final String DESTINATION = "192.168.1.1";
	private static final String SOURCE = "127.0.0.1";

	private final IProtocol protocol = new Protocol(SOURCE);

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void createProtocolWithNullSourceShouldThrowNPE() {
		exception.expect(NullPointerException.class);
		exception.expectMessage("Source's identity must NOT be null.");
		new Protocol(null);
	}

	@Test
	public void createProtocolWithEmptySourceShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Source's identity must NOT be empty.");
		new Protocol("");
	}

	@Test
	public void connect() {
		ZMsg message = protocol.connect(DESTINATION);

		assertThat(message, is(not(nullValue())));
		assertThat(message, hasSize(3));

		assertThat(message.popString(), is(DESTINATION));
		assertThat(message.popString(), is("CONNECT"));
		assertThat(message.popString(), is(SOURCE));
	}

	public void ping() {
		ZMsg message = protocol.ping(DESTINATION);

		assertThat(message, is(not(nullValue())));
		assertThat(message, hasSize(3));

		assertThat(message.popString(), is(DESTINATION));
		assertThat(message.popString(), is("PING"));
		assertThat(message.popString(), is(SOURCE));
	}

	@Test
	public void pong() {
		ZMsg message = protocol.pong(DESTINATION);

		assertThat(message, is(not(nullValue())));
		assertThat(message, hasSize(3));

		assertThat(message.popString(), is(DESTINATION));
		assertThat(message.popString(), is("PONG"));
		assertThat(message.popString(), is(SOURCE));
	}

	@Test
	public void pongDestination() {
		ZMsg message = protocol.pong(DESTINATION);

		assertThat(message, is(not(nullValue())));
		assertThat(message, hasSize(3));

		assertThat(message.popString(), is(DESTINATION));
		assertThat(message.popString(), is("PONG"));
		assertThat(message.popString(), is(SOURCE));
	}

	@Test
	public void request() {
		UUID uuid = UUID.randomUUID();
		ZMsg message = protocol.request(uuid, DESTINATION);

		assertThat(message, is(not(nullValue())));
		assertThat(message, hasSize(4));

		assertThat(message.popString(), is(DESTINATION));
		assertThat(message.popString(), is("REQUEST"));
		assertThat(message.popString(), is(SOURCE));
		assertThat(message.popString(), is(uuid.toString()));
	}

	@Test
	public void requestWithSequenceNumber() {
		UUID uuid = UUID.randomUUID();
		ZMsg message = protocol.request(uuid, 1337, DESTINATION);

		assertThat(message, is(not(nullValue())));
		assertThat(message, hasSize(4));

		assertThat(message.popString(), is(DESTINATION));
		assertThat(message.popString(), is("1337"));
		assertThat(message.popString(), is(SOURCE));
		assertThat(message.popString(), is(uuid.toString()));
	}

	@Test
	public void pingShouldBeRepliedToWithPong() {
		ZMsg reply = protocol.reply(protocol.ping(DESTINATION), PROVIDER);

		assertThat(reply, is(not(nullValue())));
		assertThat(reply, hasSize(3));

		assertThat(reply.popString(), is(DESTINATION));
		assertThat(reply.popString(), is("PONG"));
		assertThat(reply.popString(), is(SOURCE));
	}

	@Test
	public void requestShouldBeRepliedToWithOkAndExpectedPayload() {
		UUID uuid = UUID.randomUUID();
		ZMsg reply = protocol.reply(protocol.request(uuid, DESTINATION), PROVIDER);

		assertThat(reply, is(not(nullValue())));
		assertThat(reply, hasSize(5));
		assertThat(reply.popString(), is(DESTINATION));
		assertThat(reply.popString(), is("OK"));
		assertThat(reply.popString(), is(SOURCE));
		assertThat(reply.popString(), is(uuid.toString()));
		assertThat(reply.popString(), is(toZmqString(uuid)));
	}

	@Test
	public void requestWithSequenceNumberShouldBeRepliedToWithSequenceNumberAndExpectedPayload() {
		UUID uuid = UUID.randomUUID();
		ZMsg reply = protocol.reply(protocol.request(uuid, 1337, DESTINATION), PROVIDER);

		assertThat(reply, is(not(nullValue())));
		assertThat(reply, hasSize(5));
		assertThat(reply.popString(), is(DESTINATION));
		assertThat(reply.popString(), is("1337"));
		assertThat(reply.popString(), is(SOURCE));
		assertThat(reply.popString(), is(uuid.toString()));
		assertThat(reply.popString(), is(toZmqString(uuid)));
	}

	@Test
	public void requestShouldBeRepliedToWithUnknownWhenItDoesNotMatchProtocol() {
		ZMsg invalidRequest = new ZMsg();
		invalidRequest.addString("This is an invalid request");
		ZMsg reply = protocol.reply(invalidRequest, PROVIDER);

		assertThat(reply, is(not(nullValue())));
		assertThat(reply, hasSize(4));
		assertThat(reply.popString(), is("This is an invalid request"));
		assertThat(reply.popString(), is("UNKNOWN"));
		assertThat(reply.popString(), is(SOURCE));
		assertThat(reply.popString(), is(""));
	}
}
