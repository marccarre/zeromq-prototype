package com.carmatech.zeromq.utilities;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public final class Ports {
	private static final Logger LOGGER = LoggerFactory.getLogger(Ports.class);

	/** Last root port */
	private static final int MIN_PORT = 1023;
	private static final int MIN_VALID_PORT = MIN_PORT + 1;

	private static final int MAX_PORT = 65536;
	private static final int MAX_VALID_PORT = MAX_PORT - 1;

	private static final int MAX_NUMBER_OF_PORTS = MAX_PORT - MIN_PORT;

	private Ports() {
		// Pure utility class, do NOT instantiate.
	}

	public static int reserve() {
		return reserve(1).iterator().next();
	}

	public static int reserve(final int minPort, final int maxPort) {
		return reserve(1, minPort, maxPort).iterator().next();
	}

	public static Set<Integer> reserve(final int numPorts) {
		return reserve(numPorts, MIN_VALID_PORT, MAX_VALID_PORT);
	}

	public static Set<Integer> reserve(final int numPorts, final int minPort, final int maxPort) {
		validateInputs(numPorts, minPort, maxPort);
		final Set<Integer> reservedPorts = new LinkedHashSet<Integer>();

		// Hold on to the sockets which we were able to bind to a port, to guarantee we don't get the same port twice:
		final List<ServerSocket> sockets = Lists.newArrayListWithCapacity(numPorts);
		try {
			reservePorts(numPorts, minPort, maxPort, sockets);
			checkIfEnoughPorts(numPorts, minPort, maxPort, sockets);
		} finally {
			// Free all the sockets for the ports we reserved:
			freeSocketsAndAddPortsTo(reservedPorts, sockets);
			if (reservedPorts.size() == sockets.size())
				checkIfEnoughPorts(numPorts, minPort, maxPort, reservedPorts);
		}
		return reservedPorts;
	}

	private static void validateInputs(final int numPorts, final int minPort, final int maxPort) {
		checkArgument(MIN_PORT < minPort, "Invalid minimum port [%s]: ports must be strictly greater than [%s].", minPort, MIN_PORT);
		checkArgument(maxPort < MAX_PORT, "Invalid maximum port [%s]: ports must be strictly less than [%s].", maxPort, MAX_PORT);
		checkArgument(minPort < maxPort, "Invalid minimum/maximum port(s): minimum port [%s] must be strictly less than maximum port [%s].", minPort, maxPort);
		checkArgument(numPorts > 0, "Number of ports must be strictly greater than zero, but was [%s].", numPorts);
		checkArgument(numPorts < MAX_NUMBER_OF_PORTS, "Number of ports must be strictly less than [%s], but was [%s].", MAX_NUMBER_OF_PORTS, numPorts);
	}

	private static void reservePorts(final int numPorts, final int minPort, final int maxPort, final List<ServerSocket> sockets) {
		for (int port = minPort; port < maxPort; ++port) {

			final ServerSocket socket = connectTo(port);
			if (socket == null)
				continue; // port not available, try with next one.

			sockets.add(socket);
			if (sockets.size() == numPorts)
				break;
		}
	}

	private static ServerSocket connectTo(final int port) {
		try {
			return new ServerSocket(port);
		} catch (IOException e) {
			LOGGER.debug("Failed to bind to port [" + port + "]: " + e.getMessage());
			return null;
		}
	}

	private static <T> void checkIfEnoughPorts(final int numPorts, final int minPort, final int maxPort, final Collection<T> socketsOrPorts) {
		if (socketsOrPorts.size() != numPorts) {
			if (numPorts > 1)
				throw new RuntimeException(String.format("Could only reserve [%d/%d] ports: not enough ports available within [%d, %d) range.",
						socketsOrPorts.size(), numPorts, minPort, maxPort));
			else
				throw new RuntimeException(String.format("Failed to reserve port: no port available within [%d, %d) range.", minPort, maxPort));
		}
	}

	private static void freeSocketsAndAddPortsTo(final Set<Integer> reservedPorts, final List<ServerSocket> sockets) {
		for (final ServerSocket socket : sockets) {
			final int port = socket.getLocalPort();
			if (couldClose(socket))
				reservedPorts.add(port);
		}
	}

	private static boolean couldClose(final ServerSocket socket) {
		final int port = socket.getLocalPort();
		try {
			socket.close();
			return true;
		} catch (IOException e) {
			LOGGER.error("Failed to free reserved port [" + port + "]: " + e.getMessage(), e);
			return false;
		}
	}
}
