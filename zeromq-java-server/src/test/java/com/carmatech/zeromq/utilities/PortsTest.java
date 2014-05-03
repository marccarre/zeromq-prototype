package com.carmatech.zeromq.utilities;

import static com.carmatech.zeromq.utilities.FreePortMatcher.freePort;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PortsTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void reserveOnePort() {
		int port = Ports.reserve();
		assertThat(port, is(freePort()));
	}

	@Test
	public void reserveOnePortWithinRange() {
		int port = Ports.reserve(1337, 1437);
		assertThat(port, is(freePort()));
		assertThat(port, is(greaterThanOrEqualTo(1337)));
		assertThat(port, is(lessThan(1437)));
	}

	@Test
	public void reservingButNotUsingPortLeadToTheSamePortBeingReturnedTwice() {
		int port1 = Ports.reserve();
		assertThat(port1, is(freePort()));

		int port2 = Ports.reserve();
		assertThat(port2, is(freePort()));

		assertThat(port2, is(port1));
	}

	@Test
	public void reservePortNeverReturnsAPortBeingUsed() throws IOException {
		int port1 = Ports.reserve();
		assertThat(port1, is(freePort()));

		ServerSocket socket = null;
		try {
			socket = new ServerSocket(port1);
			assertThat(port1, is(not(freePort())));

			int port2 = Ports.reserve();
			assertThat(port2, is(freePort()));
			assertThat(port2, is(not(port1)));
		} finally {
			IOUtils.closeQuietly(socket);
			assertThat(port1, is(freePort()));
		}
	}

	@Test
	public void reserveUnavailablePortShouldThrowRuntimeException() throws IOException {
		exception.expect(RuntimeException.class);
		exception.expectMessage("Failed to reserve port: no port available within [1337, 1340) range.");

		assertThat(1337, is(freePort()));
		assertThat(1338, is(freePort()));
		assertThat(1339, is(freePort()));
		ServerSocket socket1 = null;
		ServerSocket socket2 = null;
		ServerSocket socket3 = null;
		try {
			socket1 = new ServerSocket(1337);
			socket2 = new ServerSocket(1338);
			socket3 = new ServerSocket(1339);
			assertThat(1337, is(not(freePort())));
			assertThat(1338, is(not(freePort())));
			assertThat(1339, is(not(freePort())));

			Ports.reserve(1337, 1340);
		} finally {
			IOUtils.closeQuietly(socket1);
			IOUtils.closeQuietly(socket2);
			IOUtils.closeQuietly(socket3);
			assertThat(1337, is(freePort()));
			assertThat(1338, is(freePort()));
			assertThat(1339, is(freePort()));
		}
	}

	@Test
	public void reserveMultiplePorts() {
		Set<Integer> ports = Ports.reserve(3);
		assertThat(ports, is(not(nullValue())));
		assertThat(ports, hasSize(3));
		for (int port : ports)
			assertThat(port, is(freePort()));
	}

	@Test
	public void reserveMultiplePortsWithinRange() {
		Set<Integer> ports = Ports.reserve(3, 1337, 1437);
		assertThat(ports, is(not(nullValue())));
		assertThat(ports, hasSize(3));
		for (int port : ports) {
			assertThat(port, is(freePort()));
			assertThat(port, is(greaterThanOrEqualTo(1337)));
			assertThat(port, is(lessThan(1437)));
		}
	}

	@Test
	public void reserveMorePortsThanAvailableShouldThrowRuntimeException() throws IOException {
		exception.expect(RuntimeException.class);
		exception.expectMessage("Could only reserve [9/10] ports: not enough ports available within [1337, 1347) range.");

		assertThat(1337, is(freePort()));
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(1337);
			assertThat(1337, is(not(freePort())));

			Ports.reserve(10, 1337, 1347);
		} finally {
			IOUtils.closeQuietly(socket);
			assertThat(1337, is(freePort()));
		}
	}

	@Test
	public void reservePortLessThanMinimumNonRootPortShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid minimum port [1023]: ports must be strictly greater than [1023].");
		Ports.reserve(1023, 1024);
	}

	@Test
	public void reservePortGreaterThanMaximumPortShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid maximum port [65536]: ports must be strictly less than [65536].");
		Ports.reserve(65536, 65536);
	}

	@Test
	public void reservePortWithMinimumPortEqualToMaximumPortShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid minimum/maximum port(s): minimum port [1337] must be strictly less than maximum port [1337].");
		Ports.reserve(1337, 1337);
	}

	@Test
	public void reservePortWithMinimumGreaterThanMaximumPortShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid minimum/maximum port(s): minimum port [2337] must be strictly less than maximum port [1337].");
		Ports.reserve(2337, 1337);
	}

	@Test
	public void reserveZeroPortsShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Number of ports must be strictly greater than zero, but was [0].");
		Ports.reserve(0);
	}

	@Test
	public void reserveLessThanZeroPortsShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Number of ports must be strictly greater than zero, but was [-1].");
		Ports.reserve(-1);
	}

	@Test
	public void reserveMoreThanMaximumNumberOfPortsShouldThrowIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Number of ports must be strictly less than [64513], but was [64513].");
		Ports.reserve(64513);
	}
}