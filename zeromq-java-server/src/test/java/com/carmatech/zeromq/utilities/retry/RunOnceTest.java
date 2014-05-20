package com.carmatech.zeromq.utilities.retry;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.carmatech.zeromq.utilities.Duration;

public class RunOnceTest {
	private final IRetryPolicy runOnce = new RunOnce(new Duration(100L, TimeUnit.MILLISECONDS));

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void typicalUsage() {

		for (int i = 0; i < 3; ++i) {
			assertThat(runOnce.hasNextAttempt(), is(true));
			assertThat(runOnce.attempts(), is(0));

			while (runOnce.hasNextAttempt()) {
				// Do something with provided duration, e.g. poll for something or sleep:
				runOnce.nextAttemptInMillis();
			}

			assertThat(runOnce.hasNextAttempt(), is(false));
			assertThat(runOnce.attempts(), is(1));

			// Re-use the same retry policy object for other runs:
			runOnce.reset();

			assertThat(runOnce.hasNextAttempt(), is(true));
			assertThat(runOnce.attempts(), is(0));
		}

	}

	@Test
	public void hasNextAttemptsIsInitiallyTrue() {
		assertThat(runOnce.hasNextAttempt(), is(true));
	}

	@Test
	public void hasNextAttemptsIsFalseAfterNextAttemptInMillisHasBeenCalled() {
		assertThat(runOnce.hasNextAttempt(), is(true));
		runOnce.nextAttemptInMillis();
		assertThat(runOnce.hasNextAttempt(), is(false));
	}

	@Test
	public void hasNextAttemptsIsTrueAgainAfterResetHasBeenCalled() {
		assertThat(runOnce.hasNextAttempt(), is(true));
		runOnce.nextAttemptInMillis();
		assertThat(runOnce.hasNextAttempt(), is(false));
		runOnce.reset();
		assertThat(runOnce.hasNextAttempt(), is(true));
	}

	@Test
	public void attemptsIsInitiallyZero() {
		assertThat(runOnce.attempts(), is(0));
	}

	@Test
	public void attemptsIsOneAfterNextAttemptInMillisHasBeenCalled() {
		assertThat(runOnce.attempts(), is(0));
		runOnce.nextAttemptInMillis();
		assertThat(runOnce.attempts(), is(1));
	}

	@Test
	public void attemptsIsZeroAgainAfterResetHasBeenCalled() {
		assertThat(runOnce.attempts(), is(0));
		runOnce.nextAttemptInMillis();
		assertThat(runOnce.attempts(), is(1));
		runOnce.reset();
		assertThat(runOnce.attempts(), is(0));
	}

	@Test
	public void nextAttemptInMillisReturnsProvidedDurationInMilliseconds() {
		assertThat(runOnce.nextAttemptInMillis(), is(100L));
	}

	@Test
	public void nextAttemptInMillisThrowsIllegalStateExceptionWhenSubsequentlyCalled() {
		exception.expect(IllegalStateException.class);
		exception.expectMessage("Number of attempts (1) exhausted.");

		assertThat(runOnce.nextAttemptInMillis(), is(100L));
		runOnce.nextAttemptInMillis(); // Second call throws exception.
	}

	@Test
	public void nextAttemptInMillisReturnsProvidedDurationInMillisecondsAgainAfterResetHasBeenCalled() {
		assertThat(runOnce.nextAttemptInMillis(), is(100L));
		runOnce.reset();
		assertThat(runOnce.nextAttemptInMillis(), is(100L));
	}

	@Test
	public void toStringShowsInternals() {
		assertThat(runOnce.toString(), is("RunOnce{duration=100 MILLISECONDS, remainingAttempts=1}"));
		runOnce.nextAttemptInMillis();
		assertThat(runOnce.toString(), is("RunOnce{duration=100 MILLISECONDS, remainingAttempts=0}"));
	}
}
