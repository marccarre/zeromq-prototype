package com.carmatech.zeromq.api;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import com.google.common.base.Function;

public class ProtocolTest {
	private static final String DESTINATION = "192.168.1.1";
	private static final String SOURCE = "127.0.0.1";

	private final IProtocol protocol = new Protocol(SOURCE);

	private final Function<UUID, byte[]> provider = new Function<UUID, byte[]>() {
		@Override
		public byte[] apply(final UUID uuid) {
			return toBytes(uuid);
		}
	};

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
		ZMsg reply = protocol.reply(protocol.ping(DESTINATION), provider);

		assertThat(reply, is(not(nullValue())));
		assertThat(reply, hasSize(3));

		assertThat(reply.popString(), is(DESTINATION));
		assertThat(reply.popString(), is("PONG"));
		assertThat(reply.popString(), is(SOURCE));
	}

	@Test
	public void requestShouldBeRepliedToWithOkAndExpectedPayload() {
		UUID uuid = UUID.randomUUID();
		ZMsg reply = protocol.reply(protocol.request(uuid, DESTINATION), provider);

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
		ZMsg reply = protocol.reply(protocol.request(uuid, 1337, DESTINATION), provider);

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
		ZMsg reply = protocol.reply(invalidRequest, provider);

		assertThat(reply, is(not(nullValue())));
		assertThat(reply, hasSize(4));
		assertThat(reply.popString(), is("This is an invalid request"));
		assertThat(reply.popString(), is("UNKNOWN"));
		assertThat(reply.popString(), is(SOURCE));
		assertThat(reply.popString(), is(""));
	}

	private String toZmqString(UUID uuid) {
		return toString(toBytes(uuid)).toUpperCase();
	}

	private byte[] toBytes(final UUID uuid) {
		final ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(uuid.hashCode());
		return buffer.array();
	}

	private String toString(final byte[] data) {
		return containsSpecialChars(data) ? new String(data, ZMQ.CHARSET) : toHexString(data);
	}

	private boolean containsSpecialChars(final byte[] data) {
		for (int i = 0; i < data.length; i++)
			if (data[i] < 32 || data[i] > 127)
				return false;
		return true;
	}

	private static final String HEXADECIMALS = "0123456789ABCDEF";

	private String toHexString(final byte[] data) {
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			int first = data[i] >>> 4 & 0xf;
			int second = data[i] & 0xf;
			builder.append(HEXADECIMALS.charAt(first));
			builder.append(HEXADECIMALS.charAt(second));
		}
		return builder.toString();
	}
}
