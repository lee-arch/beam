package com.my;

import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RecordReflectorLoader {
	private final ConcurrentMap<String, RecordReflector> repositry = new ConcurrentHashMap<>();

	public RecordReflector get(String eventTarget) {
		return repositry.computeIfAbsent(eventTarget, _eventTarget -> {
			for (RecordReflector reflector : ServiceLoader.load(RecordReflector.class)) {
				if (_eventTarget.equals(reflector.eventTarget())) {
					return reflector;
				}
			}
			return null;
		});
	}
}
