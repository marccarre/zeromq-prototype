package com.carmatech.zeromq.utilities;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Ticker;

/**
 * Decorator around {@link com.google.common.base.Ticker} which reads time in milliseconds.
 */
public class MillisecondsTicker extends Ticker implements IMillisecondsTicker {
	private final Ticker ticker;

	public MillisecondsTicker(final Ticker ticker) {
		this.ticker = checkNotNull(ticker);
	}

	@Override
	public long read() {
		return ticker.read();
	}

	@Override
	public long readMillis() {
		return TimeUnit.NANOSECONDS.toMillis(ticker.read());
	}
}
