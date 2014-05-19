package com.carmatech.zeromq.utilities;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

public class Duration {
	private static final String WHITESPACE = " ";

	private final long duration;
	private final TimeUnit unit;

	public Duration(final long duration, final TimeUnit unit) {
		this.duration = duration;
		this.unit = checkNotNull(unit);
	}

	public long duration() {
		return duration;
	}

	public TimeUnit unit() {
		return unit;
	}

	@Override
	public String toString() {
		return duration + WHITESPACE + unit.toString();
	}

	public long toNanos() {
		return unit.toNanos(duration);
	}

	public long toMicros() {
		return unit.toMicros(duration);
	}

	public long toMillis() {
		return unit.toMillis(duration);
	}

	public long toSeconds() {
		return unit.toSeconds(duration);
	}

	public long toMinutes() {
		return unit.toMinutes(duration);
	}

	public long toHours() {
		return unit.toHours(duration);
	}

	public long toDays() {
		return unit.toDays(duration);
	}

	public long convert(long sourceDuration, TimeUnit sourceUnit) {
		return unit.convert(sourceDuration, sourceUnit);
	}

	public void sleep() throws InterruptedException {
		unit.sleep(duration);
	}

	public void sleepUninterruptibly() {
		Uninterruptibles.sleepUninterruptibly(duration, unit);
	}
}
