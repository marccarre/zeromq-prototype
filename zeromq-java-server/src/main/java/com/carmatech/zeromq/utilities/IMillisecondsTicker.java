package com.carmatech.zeromq.utilities;

/**
 * Interface for decorators around {@link com.google.common.base.Ticker} reading time in milliseconds.
 */
public interface IMillisecondsTicker {

	/**
	 * Read ticker's time and return it in milliseconds.
	 * 
	 * @return ticker's time in milliseconds.
	 */
	long readMillis();

}