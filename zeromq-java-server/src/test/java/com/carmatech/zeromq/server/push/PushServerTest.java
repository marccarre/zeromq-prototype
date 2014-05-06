package com.carmatech.zeromq.server.push;

import static com.carmatech.zeromq.utilities.ProtocolPayloadUtilities.PROVIDER;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.carmatech.zeromq.server.IServer;
import com.google.common.collect.Queues;

public class PushServerTest {
	public static void main(final String[] argv) throws IOException {
		final BlockingQueue<Pair<UUID, byte[]>> queue = Queues.newArrayBlockingQueue(1024);
		final IServer server = new PushServer(8888, queue);
		server.run();

		System.out.println("Press <Enter> to generate message or <q + Enter> exit.");

		final Scanner inputReader = new Scanner(System.in);
		String input;
		while (true) {
			input = inputReader.nextLine();
			if (input.contains("q"))
				break;

			final UUID uuid = UUID.randomUUID();
			queue.add(ImmutablePair.of(uuid, PROVIDER.apply(uuid)));
			System.out.println("Produced message with key [" + uuid + "].");
		}

		inputReader.close();
		server.close();
		System.out.println("Bye!");
	}
}
