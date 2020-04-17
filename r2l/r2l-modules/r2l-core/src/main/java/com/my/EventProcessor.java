package com.my;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.ignite.IgniteCache;

import com.r2l.model.Binary;
import com.r2l.model.common.colf.OutboxData;
import com.r2l.model.common.colf.TxnCacheKey;
import com.r2l.model.common.colf.TxnCacheValue;

public class EventProcessor {
	private final String eventKafkaServers;
	private final String eventTopic;
	private final String txnKafkaServers;
	private final String txnTopic;
	private final IgniteCache<Binary, byte[]> txnCache;

	private final CachedRecordsCounter recordsCounter = new CachedRecordsCounter();

	public EventProcessor(String eventKafkaServers, String eventTopic,
			String txnKafkaServers, String txnTopic,
			IgniteCache<Binary, byte[]> txnCache) {

		this.eventKafkaServers = eventKafkaServers;
		this.eventTopic = eventTopic;
		this.txnKafkaServers = txnKafkaServers;
		this.txnTopic = txnTopic;
		this.txnCache = txnCache;
	}
	public void initialize(Pipeline pipeline) {
		pipeline //
			.apply( //
				"consume message from kafka", //
				KafkaIO.readBytes() //
					.withBootstrapServers(eventKafkaServers) //
					.withTopics(Arrays.asList(eventTopic)) //
			) //
			.apply( //
				"deserialize kafka message to colfer object(OutboxData)", //
				MapElements.via(new SimpleFunction<KafkaRecord<byte[], byte[]>, OutboxData>() {
					/** serialVersionUID */
					private static final long serialVersionUID = 2598168889853267851L;
					@Override
					public OutboxData apply(KafkaRecord<byte[], byte[]> input) {
						return new OutboxData().unmarshal(input.getKV().getValue());
					}
				}) //
			) //
			.apply( //
				"cache transaction data", //
				MapElements.via(new SimpleFunction<OutboxData, OutboxData>() {
					/** serialVersionUID */
					private static final long serialVersionUID = 3698168889823667852L;
					@Override
					public OutboxData apply(OutboxData input) {
						//put cache to ignite
						TxnCacheKey cacheKey = new TxnCacheKey()
								.withTransactionId(input.getCommand().getTransactionId())
								.withSerialNo(input.getEvent().getSerialNo());
						TxnCacheValue cacheValue = new TxnCacheValue()
								.withEvent(input.getEvent())
								.withRecord(input.getRecord());
						txnCache.put(Binary.of(cacheKey), cacheValue.marshal());

						recordsCounter.increase(input.getCommand().getTransactionId());
						return input;
					}
				}) //
			) //
			.apply(Filter.by((OutboxData input) -> { //
				return input.getCommand().getMaxSerialNo() == input.getEvent().getSerialNo();
			})) //
			.apply( //
				"prepare payload", //
				MapElements.via(new SimpleFunction<OutboxData, KV<byte[], byte[]>>() {
					/** serialVersionUID */
					private static final long serialVersionUID = 5758168889823667853L;
					@Override
					public KV<byte[], byte[]> apply(OutboxData input) {
						String txnId = input.getCommand().getTransactionId();
						while(true) {
							if(recordsCounter.get(txnId) >= input.getCommand().getMaxSerialNo()) {
								recordsCounter.remove(txnId);
								break;
							}
							try {
								TimeUnit.MILLISECONDS.sleep(1);
							} catch (InterruptedException e) {
							}
						}
						return KV.of(
								txnId.getBytes(),
								input.getCommand().marshal()
								);
					}
				}) //
			) //
			.apply( //
				"produce message",
				KafkaIO.<byte[], byte[]>write()
	              .withBootstrapServers(txnKafkaServers)
	              .withTopic(txnTopic)
			) //
		;
	}

	private static class CachedRecordsCounter {
		private final ConcurrentMap<String, AtomicInteger> cachedCounter = new ConcurrentHashMap<>();

		private AtomicInteger getCounter(String txnId) {
			return cachedCounter.computeIfAbsent(txnId, _txnId -> {
				return new AtomicInteger(1);
			});
		}

		public int get(String txnId) {
			return getCounter(txnId).get();
		}

		public void increase(String txnId) {
			getCounter(txnId).getAndIncrement();
		}

		public void remove(String txnId) {
			cachedCounter.remove(txnId);
		}
	}
}
