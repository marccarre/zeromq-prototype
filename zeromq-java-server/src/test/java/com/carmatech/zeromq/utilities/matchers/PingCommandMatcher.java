package com.carmatech.zeromq.utilities.matchers;

import static com.carmatech.zeromq.api.Command.PING;
import static com.google.common.base.Preconditions.checkArgument;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.zeromq.ZMsg;

import com.google.common.base.Objects;

/**
 * Is the ZMsg message a PING?
 */
public class PingCommandMatcher extends BaseMatcher<ZMsg> {
	private final String destination;
	private final String source;
	private String message;

	public PingCommandMatcher(final String destination, final String source) {
		this.destination = destination;
		this.source = source;
	}

	@Override
	public boolean matches(final Object pingCommand) {
		checkArgument(pingCommand instanceof ZMsg);

		final ZMsg ping = ((ZMsg) pingCommand).duplicate(); // Copy the request to be able to reply to it in the rest of the code.

		if (ping == null)
			return falseWithMessage("ping command is null.");

		if (ping.size() != 3)
			return falseWithMessage(String.format("ping command should be of size 3, but was of size %d.", ping.size()));

		String frame = ping.popString();
		if (!Objects.equal(frame, destination))
			return falseWithMessage(String.format("ping command's 1st frame should be [%d], but was [%s].", destination, frame));

		frame = ping.popString();
		if (!Objects.equal(frame, PING.toString()))
			return falseWithMessage(String.format("ping command's 2nd frame should be [%d], but was [%s].", PING.toString(), frame));

		frame = ping.popString();
		if (!Objects.equal(frame, source))
			return falseWithMessage(String.format("ping command's 3rd frame should be [%d], but was [%s].", source, frame));

		return true;
	}

	private boolean falseWithMessage(final String message) {
		this.message = message;
		return false;
	}

	@Override
	public void describeTo(Description description) {
		description.appendText(message);
	}

	/**
	 * Creates a matcher that matches if provided ZMsg is a valid PING command.
	 * <p/>
	 * For example:
	 * 
	 * <pre>
	 * assertThat(message, isPingWith(destination, source))
	 * </pre>
	 */
	@Factory
	public static Matcher<ZMsg> isPingCommandWith(final String destination, final String source) {
		return new PingCommandMatcher(destination, source);
	}
}