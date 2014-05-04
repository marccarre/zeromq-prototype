package com.carmatech.zeromq.utilities;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.google.common.base.Ticker;

public class MillisecondsTickerTest {
	private static final long TIMESTAMP_IN_NANOS = 1399189598514000000L;
	private final MillisecondsTicker ticker = new MillisecondsTicker(new Ticker() {
		@Override
		public long read() {
			return TIMESTAMP_IN_NANOS;
		}
	});

	@Test
	public void readShouldReturnTickersTimeInNanoseconds() {
		assertThat(ticker.read(), is(TIMESTAMP_IN_NANOS));
	}

	@Test
	public void readMillisShouldReturnTickersTimeInMilliseconds() {
		assertThat(ticker.readMillis(), is(TIMESTAMP_IN_NANOS / 1_000_000));
	}
}
