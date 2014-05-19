package com.carmatech.zeromq.utilities;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class DurationTest {
	@Test
	public void durationShouldHoldProvidedValues() {
		Duration duration = new Duration(1, TimeUnit.MILLISECONDS);
		assertThat(duration.duration(), is(1L));
		assertThat(duration.unit(), is(TimeUnit.MILLISECONDS));
	}

	@Test
	public void durationShouldPrintNicely() {
		Duration duration = new Duration(1337, TimeUnit.SECONDS);
		assertThat(duration.toString(), is("1337 SECONDS"));
	}

	@Test
	public void toNanosShouldConvertDurationToNanoseconds() {
		Duration duration = new Duration(1, TimeUnit.SECONDS);
		assertThat(duration.toNanos(), is(1_000_000_000L));
	}

	@Test
	public void toMicrosShouldConvertDurationToMicroseconds() {
		Duration duration = new Duration(1, TimeUnit.SECONDS);
		assertThat(duration.toMicros(), is(1_000_000L));
	}

	@Test
	public void toMillisShouldConvertDurationToMilliseconds() {
		Duration duration = new Duration(1, TimeUnit.SECONDS);
		assertThat(duration.toMillis(), is(1_000L));
	}

	@Test
	public void toSecondsShouldConvertDurationToSeconds() {
		Duration duration = new Duration(1, TimeUnit.HOURS);
		assertThat(duration.toSeconds(), is(3_600L));
	}

	@Test
	public void toMinutesShouldConvertDurationToMinutes() {
		Duration duration = new Duration(1, TimeUnit.HOURS);
		assertThat(duration.toMinutes(), is(60L));
	}

	@Test
	public void toHoursShouldConvertDurationToHours() {
		Duration duration = new Duration(1, TimeUnit.DAYS);
		assertThat(duration.toHours(), is(24L));
	}

	@Test
	public void toDaysShouldConvertDurationToHours() {
		Duration duration = new Duration(48, TimeUnit.HOURS);
		assertThat(duration.toDays(), is(2L));
	}
}
