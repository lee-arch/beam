package com.sample;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Sample {
	public static void main(String[] args) {
		final int params = 10;

		String[] list = Arrays.asList(new String[params])
				.stream()
				.map(s -> "?")
				.collect(Collectors.toList())
				.toArray(new String[1]);


		String txn =
				String.join(", ", list);

		System.out.println(txn);

		System.exit(0);
	}
}
