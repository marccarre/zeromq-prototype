package com.carmatech.zeromq.utilities.retry;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.concurrent.NotThreadSafe;

import com.carmatech.zeromq.utilities.Duration;
import com.google.common.base.Objects;

@NotThreadSafe
public class RunOnce implements IRetryPolicy {
	private static final int ONE = 1;

	private int remainingAttempts = ONE;
	private final Duration duration;

	public RunOnce(final Duration duration) {
		this.duration = checkNotNull(duration);
	}

	@Override
	public boolean hasNextAttempt() {
		return remainingAttempts == ONE;
	}

	@Override
	public long nextAttemptInMillis() {
		if (remainingAttempts == ONE) {
			--remainingAttempts;
			return duration.toMillis();
		} else {
			throw new IllegalStateException("Number of attempts (" + ONE + ") exhausted.");
		}
	}

	@Override
	public int attempts() {
		return hasNextAttempt() ? 0 : 1;
	}

	@Override
	public void reset() {
		remainingAttempts = ONE;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("duration", duration).add("remainingAttempts", remainingAttempts).toString();
	}
}
