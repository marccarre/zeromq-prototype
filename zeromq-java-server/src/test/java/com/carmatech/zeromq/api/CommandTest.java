package com.carmatech.zeromq.api;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class CommandTest {
	@Test
	public void numericMessageIsParsedAsNumbered() {
		assertThat(Command.parse("123"), is(Command.SEQUENCE_NUMBER));
	}

	@Test
	public void invalidMessageIsParsedAsUnknown() {
		assertThat(Command.parse("WTF is that?!"), is(Command.UNKNOWN));
	}

	@Test
	public void emptyMessageIsParsedAsUnknown() {
		assertThat(Command.parse(""), is(Command.UNKNOWN));
	}

	@Test
	public void nullMessageIsParsedAsUnknown() {
		assertThat(Command.parse(null), is(Command.UNKNOWN));
	}

	@Test
	public void validMessagesAreParsedToEnumValues() {
		assertThat(Command.parse("CONNECT"), is(Command.CONNECT));
		assertThat(Command.parse("PING"), is(Command.PING));
		assertThat(Command.parse("PONG"), is(Command.PONG));
		assertThat(Command.parse("REQUEST"), is(Command.REQUEST));
		assertThat(Command.parse("OK"), is(Command.OK));
		assertThat(Command.parse("ERROR"), is(Command.ERROR));
	}
}
