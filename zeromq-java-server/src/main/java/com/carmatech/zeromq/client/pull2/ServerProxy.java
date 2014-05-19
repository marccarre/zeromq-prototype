package com.carmatech.zeromq.client.pull2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carmatech.zeromq.utilities.Duration;
import com.google.common.base.Objects;
import com.google.common.base.Ticker;

public class ServerProxy {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerProxy.class);

	private final String endpoint;
	private final Duration pingFrequency;
	private final Ticker ticker;
	private final AtomicBoolean isActive;
	private final AtomicLong pingAt;

	public ServerProxy(final String endpoint, final Duration pingFrequency, final Ticker ticker) {
		this.endpoint = endpoint;
		this.pingFrequency = checkNotNull(pingFrequency, "Ping frequency must NOT be null.");
		this.ticker = checkNotNull(ticker, "Ticker must NOT be null.");
		isActive = new AtomicBoolean(false);
		pingAt = new AtomicLong(now() - 1L);
	}

	public ServerProxy(final String endpoint, final Duration pingFrequency) {
		this(endpoint, pingFrequency, Ticker.systemTicker());
	}

	public boolean shouldBePinged() {
		return pingAt() < now();
	}

	public boolean shouldBeDeactivated() {
		return pingAt() < now() - pingFrequency.toNanos();
	}

	public long pingAt() {
		return pingAt.get();
	}

	public String endpoint() {
		return endpoint;
	}

	public void activate() {
		if (isActive.compareAndSet(false, true)) {
			LOGGER.debug("Activated " + this);
		} else {
			isActive.set(true);
		}
	}

	public void deactivate() {
		if (isActive.compareAndSet(true, false)) {
			LOGGER.debug("Desactivated " + this);
		} else {
			isActive.set(false);
		}
	}

	public void refresh() {
		pingAt.getAndSet(now() + pingFrequency.toNanos());
		LOGGER.debug("Refreshed " + this);
	}

	public boolean isActive() {
		return isActive.get();
	}

	private long now() {
		return ticker.read();
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("endpoint", endpoint).add("isActive", isActive).add("pingAt", pingAt).toString();
	}
}
