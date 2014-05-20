package com.carmatech.zeromq.api;

import java.util.UUID;

import org.zeromq.ZMsg;

import com.google.common.base.Function;

public interface IProtocol {

	ZMsg connect(final String destination);

	ZMsg ping(final String destination);

	ZMsg pong(final String destination);

	ZMsg request(final String destination, final UUID uuid);

	ZMsg request(final String destination, final UUID uuid, final int sequenceNumber);

	ZMsg reply(final ZMsg request, Function<UUID, byte[]> provider);

	ZMsg error(final String destination, final String errorMessage);

}