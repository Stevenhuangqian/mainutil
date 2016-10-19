package com.efun.mainland.util;

public class HashUtil {

	public static final int getCapacity(int size) {
		return getCapacity(size, 0.75f);
	}

	public static final int getCapacity(int size, float loadFactor) {
		return (int) Math.ceil(size / loadFactor) + 1;
	}
}
