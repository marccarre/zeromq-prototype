package com.carmatech.zeromq.api;

import java.util.UUID;

import org.zeromq.ZMsg;

import com.google.common.base.Function;

public interface IProtocol {

	ZMsg connect(final String destination);

	ZMsg ping(final String destination);

	ZMsg pong(final String destination);

	ZMsg request(final UUID uuid, final String destination);

	ZMsg request(final UUID uuid, final int sequenceNumber, final String destination);

	ZMsg reply(final ZMsg request, Function<UUID, byte[]> provider);

}