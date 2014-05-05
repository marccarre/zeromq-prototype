package com.carmatech.zeromq.server.pull;

import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.PROVIDER;

import java.io.IOException;

import com.carmatech.zeromq.server.IServer;

public class PullServerTest {
	public static void main(final String[] argv) throws IOException {
		final IServer server = new PullServer(8888, PROVIDER);
		server.run();

		System.out.println("Press <Enter> to exit.");
		System.in.read();

		server.close();
		System.out.println("Bye!");
	}
}
