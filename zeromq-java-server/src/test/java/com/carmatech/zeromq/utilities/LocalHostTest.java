package com.carmatech.zeromq.utilities;

import static com.carmatech.zeromq.utilities.matchers.RegexMatcher.matches;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class LocalHostTest {
	private static final String IP_ADDRESS_PATTERN = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";

	@Test
	public void ipShouldReturnLocalHostIpAddressDigits() {
		assertThat(LocalHost.IP, matches(IP_ADDRESS_PATTERN));
	}

	@Test
	public void hostAndIpShouldReturnLocalHostIpAddressDigits() {
		assertThat(LocalHost.HOST_AND_IP, matches("[\\w\\.-]+/" + IP_ADDRESS_PATTERN));
	}
}
