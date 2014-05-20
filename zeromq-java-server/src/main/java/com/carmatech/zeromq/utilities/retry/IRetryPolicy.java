package com.carmatech.zeromq.utilities.retry;

public interface IRetryPolicy {

	boolean hasNextAttempt();

	long nextAttemptInMillis();

	int attempts();

	void reset();

}
