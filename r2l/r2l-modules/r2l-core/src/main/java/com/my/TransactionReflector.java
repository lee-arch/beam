package com.my;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.ignite.IgniteCache;

import com.r2l.DataSourceFactory;
import com.r2l.model.Binary;
import com.r2l.model.common.colf.OutboxCommand;
import com.r2l.model.common.colf.TxnCacheKey;
import com.r2l.model.common.colf.TxnCacheValue;

public class TransactionReflector {
	private final String txnKafkaServers;
	private final String txnTopic;
	private final IgniteCache<Binary, byte[]> txnCache;
	private final String uri;
	private final String user;
	private final String password;

	public TransactionReflector(String txnKafkaServers,
			String txnTopic,
			IgniteCache<Binary, byte[]> txnCache,
			String uri,
			String user,
			String password) {

		this.txnKafkaServers = txnKafkaServers;
		this.txnTopic = txnTopic;
		this.txnCache = txnCache;
		this.uri = uri;
		this.user = user;
		this.password = password;
	}

	public void initialize(Pipeline pipeline) {
		pipeline //
			.apply( //
				"consume message from kafka", //
				KafkaIO.readBytes() //
					.withBootstrapServers(txnKafkaServers) //
					.withTopics(Arrays.asList(txnTopic)) //
			) //
			.apply( //
				"deserialize kafka message to colfer object(OutboxCommand)", //
				MapElements.via(new SimpleFunction<KafkaRecord<byte[], byte[]>, OutboxCommand>() {
					/** serialVersionUID */
					private static final long serialVersionUID = 5678168889853267855L;
					@Override
					public OutboxCommand apply(KafkaRecord<byte[], byte[]> input) {
						return new OutboxCommand().unmarshal(input.getKV().getValue());
					}
				}) //
			) //
			.apply( //
				"reflect transaction data to dest db", //
				MapElements.via(new RecordsReflector(txnCache, uri, user, password)) //
			) //


			;
	}


	private static class RecordsReflector extends SimpleFunction<OutboxCommand, OutboxCommand> {
		/** serialVersionUID */
		private static final long serialVersionUID = 8523249688010187382L;

		private final IgniteCache<Binary, byte[]> txnCache;
		private final String uri;
		private final String user;
		private final String password;

		private final RecordReflectorLoader loader = new RecordReflectorLoader();

		public RecordsReflector(IgniteCache<Binary, byte[]> txnCache,
				String uri,
				String user,
				String password) {
			this.txnCache = txnCache;
			this.uri = uri;
			this.user = user;
			this.password = password;
		}

		@Override
		public OutboxCommand apply(OutboxCommand input) {
			try(Connection connection = DataSourceFactory.get(uri, user, password).getConnection()) {
				for(int i = 1; i <= input.getMaxSerialNo(); i++) {
					TxnCacheValue cacheValue = new TxnCacheValue().unmarshal(
						txnCache.get(Binary.of(
									new TxnCacheKey()
										.withTransactionId(input.getTransactionId())
										.withSerialNo(i)
										)
									)
					);

					RecordReflector reflector = loader.get(cacheValue.getEvent().getEventTarget());

					reflector.createUpdateStatement(connection, cacheValue.getRecord()).execute();
				}
				connection.commit();
			} catch (SQLException e) {
			}

			return input;
		}
	}
}
