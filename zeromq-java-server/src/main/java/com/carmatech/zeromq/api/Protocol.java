package com.carmatech.zeromq.api;

import static com.carmatech.zeromq.api.Command.CONNECT;
import static com.carmatech.zeromq.api.Command.ERROR;
import static com.carmatech.zeromq.api.Command.OK;
import static com.carmatech.zeromq.api.Command.PING;
import static com.carmatech.zeromq.api.Command.PONG;
import static com.carmatech.zeromq.api.Command.REQUEST;
import static com.carmatech.zeromq.api.Command.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import com.google.common.base.Function;

/**
 * {@link Protocol} follows this general structure: <br/>
 * 
 * <pre>
 * +--------------------+
 * | 0: ID destination  |
 * +--------------------+
 * | 1: Command         |
 * +--------------------+
 * | 2: ID source       |
 * +--------------------+
 * |                    |
 * | 3: Arguments       |
 * |                    |
 * +--------------------+
 * </pre>
 */
public final class Protocol implements IProtocol {
	private static final Logger LOGGER = LoggerFactory.getLogger(Protocol.class);

	private final String source;

	public Protocol(final String source) {
		this.source = validate(source, "Source's identity");
	}

	private static String validate(final String value, final String name) {
		checkNotNull(value, "%s must NOT be null.", name);
		checkArgument(!value.isEmpty(), "%s must NOT be empty.", name);
		return value;
	}

	/**
	 * Creates a message according to the specified structure.
	 * 
	 * @param command
	 *            Command to set in message.
	 * @param destination
	 *            Destination of this message.
	 * @return Message pre-configured with provided destination, provided command and current agent's identity.
	 */
	private ZMsg create(final String command, final String destination) {
		final ZMsg message = new ZMsg();
		message.add(destination);
		message.add(command);
		message.add(source);
		return message;
	}

	private ZMsg create(final String command, final ZFrame destination) {
		final ZMsg message = new ZMsg();
		message.add(destination);
		message.add(command);
		message.add(source);
		return message;
	}

	private ZMsg create(final Command command, final ZFrame destination) {
		final ZMsg message = new ZMsg();
		message.add(destination);
		message.add(command.toString());
		message.add(source);
		return message;
	}

	private ZMsg create(final Command command, final String destination) {
		return create(command.toString(), destination);
	}

	private ZMsg create(final int command, final String destination) {
		return create(Integer.toString(command), destination);
	}

	/**
	 * <pre>
	 * +--------------------+
	 * | 0: ID destination  |
	 * +--------------------+
	 * | 1: CONNECT         |
	 * +--------------------+
	 * | 2: ID source       |
	 * +--------------------+
	 * </pre>
	 */
	@Override
	public ZMsg connect(final String destination) {
		validate(destination, "Destination's endpoint");
		final ZMsg message = create(CONNECT, destination);
		return message;
	}

	/**
	 * <pre>
	 * +--------------------+
	 * | 0: ID destination  |
	 * +--------------------+
	 * | 1: PING            |
	 * +--------------------+
	 * | 2: ID source       |
	 * +--------------------+
	 * </pre>
	 */
	@Override
	public ZMsg ping(final String destination) {
		validate(destination, "Destination's endpoint");
		final ZMsg ping = create(PING, destination);
		return ping;
	}

	/**
	 * <pre>
	 * +--------------------+
	 * | 0: ID destination  |
	 * +--------------------+
	 * | 1: PONG            |
	 * +--------------------+
	 * | 2: ID source       |
	 * +--------------------+
	 * </pre>
	 */
	@Override
	public ZMsg pong(final String destination) {
		validate(destination, "Destination's endpoint");
		return create(PONG, destination);
	}

	/**
	 * <pre>
	 * +--------------------+
	 * | 0: ID destination  |
	 * +--------------------+
	 * | 1: REQUEST         |
	 * +--------------------+
	 * | 2: ID source       |
	 * +--------------------+
	 * | 3: UUID            |
	 * +--------------------+
	 * </pre>
	 */
	@Override
	public ZMsg request(final String destination, final UUID uuid) {
		checkNotNull(uuid, "%s must not be null.", "Requested UUID");
		validate(destination, "Destination's endpoint");
		final ZMsg request = create(REQUEST, destination);
		request.add(uuid.toString());
		return request;
	}

	/**
	 * <pre>
	 * +--------------------+
	 * | 0: ID destination  |
	 * +--------------------+
	 * | 1: ID request      |
	 * +--------------------+
	 * | 2: ID source       |
	 * +--------------------+
	 * | 3: UUID            |
	 * +--------------------+
	 * </pre>
	 */
	@Override
	public ZMsg request(final String destination, final UUID uuid, final int sequenceNumber) {
		checkNotNull(uuid, "%s must not be null.", "Requested UUID");
		validate(destination, "Destination's endpoint");
		final ZMsg request = create(sequenceNumber, destination);
		request.add(uuid.toString());
		return request;
	}

	@Override
	public ZMsg reply(final ZMsg request, final Function<UUID, byte[]> provider) {
		checkNotNull(provider, "%s must not be null.", "Provider");
		try {
			log(request);
			final ZMsg reply = tryReply(request, provider);
			log(reply);
			return reply;
		} finally {
			request.destroy(); // Safeguard: free resources if not already done earlier.
		}
	}

	private ZMsg tryReply(final ZMsg request, final Function<UUID, byte[]> provider) {
		final ZFrame destination = request.pop();
		final String commandAsString = request.popString();
		final Command command = Command.parse(commandAsString);
		final String source = request.popString();

		return chooseReply(request, provider, destination, commandAsString, command, source);
	}

	private ZMsg chooseReply(final ZMsg request, final Function<UUID, byte[]> provider, final ZFrame destination, final String commandAsString,
			final Command command, final String source) {
		switch (command) {
		case PING:
			request.destroy();
			return create(PONG, destination);
		case REQUEST:
			return doReply(request, provider, destination);
		case SEQUENCE_NUMBER:
			return doReply(request, provider, destination, commandAsString);
		default:
			request.destroy();
			return unknown(commandAsString, destination, source);
		}
	}

	private ZMsg doReply(final ZMsg request, final Function<UUID, byte[]> provider, final ZFrame destination, final String sequenceNumberAsString) {
		final UUID uuid = extractUuidAndDestroy(request);
		return buildReply(uuid, provider, create(sequenceNumberAsString, destination));
	}

	private ZMsg doReply(final ZMsg request, final Function<UUID, byte[]> provider, final ZFrame destination) {
		final UUID uuid = extractUuidAndDestroy(request);
		return buildReply(uuid, provider, create(OK, destination));
	}

	private UUID extractUuidAndDestroy(final ZMsg request) {
		final UUID uuid = UUID.fromString(request.popString());
		request.destroy(); // Free request's resources ASAP.
		return uuid;
	}

	private ZMsg buildReply(final UUID uuid, final Function<UUID, byte[]> provider, final ZMsg reply) {
		reply.add(uuid.toString());
		reply.add(provider.apply(uuid));
		return reply;
	}

	private ZMsg unknown(final String commandAsString, final ZFrame destination, final String source) {
		final Command unknown = UNKNOWN;
		LOGGER.warn("Failed to handle [{}] request from [{}]: replying with [{}].", commandAsString, source, unknown);
		final ZMsg reply = create(unknown, destination);
		reply.add(commandAsString);
		return reply;
	}

	private void log(final ZMsg message) {
		if (!LOGGER.isDebugEnabled())
			return;

		if (message == null) {
			LOGGER.debug("ZMsg object was null.");
		} else {
			final StringBuilder builder = new StringBuilder();
			message.dump(builder);
			LOGGER.debug(builder.toString());
		}
	}

	/**
	 * <pre>
	 * +--------------------+
	 * | 0: ID destination  |
	 * +--------------------+
	 * | 1: ERROR           |
	 * +--------------------+
	 * | 2: ID source       |
	 * +--------------------+
	 * | 3: Message         |
	 * +--------------------+
	 * </pre>
	 */
	@Override
	public ZMsg error(final String destination, final String errorMessage) {
		validate(destination, "Destination's endpoint");
		final ZMsg error = create(ERROR, destination);
		error.add(errorMessage);
		return error;
	}
}
