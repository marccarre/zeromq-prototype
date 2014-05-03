package com.carmatech.zeromq.utilities;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.commons.io.IOUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Is the port free?
 */
public class FreePortMatcher extends BaseMatcher<Integer> {

	@Override
	public boolean matches(Object item) {
		final int port = Integer.parseInt(item.toString());
		return couldConnectTo(port);
	}

	private boolean couldConnectTo(final int port) {
		try {
			final ServerSocket socket = new ServerSocket(port);
			IOUtils.closeQuietly(socket);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("free port");
	}

	/**
	 * Creates a matcher that matches if examined port can be connected to.
	 * <p/>
	 * For example:
	 * 
	 * <pre>
	 * assertThat(1337, is(freePort())
	 * </pre>
	 */
	@Factory
	public static Matcher<Integer> freePort() {
		return new FreePortMatcher();
	}
}