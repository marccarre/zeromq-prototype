package com.carmatech.zeromq.utilities;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Does the string matches regex?
 */
public class RegexMatcher extends BaseMatcher<String> {
	private final String regex;

	public RegexMatcher(final String regex) {
		this.regex = regex;
	}

	@Override
	public boolean matches(Object item) {
		return item.toString().matches(regex);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("string matching regex " + this.regex);
	}

	/**
	 * Creates a matcher that matches if examined string matches the provided regex.
	 * <p/>
	 * For example:
	 * 
	 * <pre>
	 * assertThat("Hello", matches(".*ello))
	 * </pre>
	 */
	@Factory
	public static Matcher<String> matches(final String regex) {
		return new RegexMatcher(regex);
	}
}