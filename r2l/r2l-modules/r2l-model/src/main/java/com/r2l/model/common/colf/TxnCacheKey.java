package com.r2l.model.common.colf;


// Code generated by colf(1); DO NOT EDIT.


import static java.lang.String.format;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.InputMismatchException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;


/**
 * Data bean with built-in serialization support.

 * @author generated by colf(1)
 * @see <a href="https://github.com/pascaldekloe/colfer">Colfer's home</a>
 */
@javax.annotation.Generated(value="colf(1)", comments="Colfer from schema file C:\\app\\pleiades\\workspace\\com.r2l-master\\r2l\\r2l-modules\\r2l-model\\src\\main\\colfer\\colfer.colf")
public class TxnCacheKey extends com.r2l.model.ColferObject implements Serializable {

	/** The upper limit for serial byte sizes. */
	public static int colferSizeMax = 16 * 1024 * 1024;




	public String transactionId;

	public int serialNo;


	/** Default constructor */
	public TxnCacheKey() {
		init();
	}


	/** Colfer zero values. */
	private void init() {
		transactionId = "";
	}

	/**
	 * {@link #reset(InputStream) Reusable} deserialization of Colfer streams.
	 */
	public static class Unmarshaller {

		/** The data source. */
		protected InputStream in;

		/** The read buffer. */
		public byte[] buf;

		/** The {@link #buf buffer}'s data start index, inclusive. */
		protected int offset;

		/** The {@link #buf buffer}'s data end index, exclusive. */
		protected int i;


		/**
		 * @param in the data source or {@code null}.
		 * @param buf the initial buffer or {@code null}.
		 */
		public Unmarshaller(InputStream in, byte[] buf) {
			// TODO: better size estimation
			if (buf == null || buf.length == 0)
				buf = new byte[Math.min(TxnCacheKey.colferSizeMax, 2048)];
			this.buf = buf;
			reset(in);
		}

		/**
		 * Reuses the marshaller.
		 * @param in the data source or {@code null}.
		 * @throws IllegalStateException on pending data.
		 */
		public void reset(InputStream in) {
			if (this.i != this.offset) throw new IllegalStateException("colfer: pending data");
			this.in = in;
			this.offset = 0;
			this.i = 0;
		}

		/**
		 * Deserializes the following object.
		 * @return the result or {@code null} when EOF.
		 * @throws IOException from the input stream.
		 * @throws SecurityException on an upper limit breach defined by {@link #colferSizeMax}.
		 * @throws InputMismatchException when the data does not match this object's schema.
		 */
		public TxnCacheKey next() throws IOException {
			if (in == null) return null;

			while (true) {
				if (this.i > this.offset) {
					try {
						TxnCacheKey o = new TxnCacheKey();
						this.offset = o.unmarshal(this.buf, this.offset, this.i);
						return o;
					} catch (BufferUnderflowException e) {
					}
				}
				// not enough data

				if (this.i <= this.offset) {
					this.offset = 0;
					this.i = 0;
				} else if (i == buf.length) {
					byte[] src = this.buf;
					// TODO: better size estimation
					if (offset == 0) this.buf = new byte[Math.min(TxnCacheKey.colferSizeMax, this.buf.length * 4)];
					System.arraycopy(src, this.offset, this.buf, 0, this.i - this.offset);
					this.i -= this.offset;
					this.offset = 0;
				}
				assert this.i < this.buf.length;

				int n = in.read(buf, i, buf.length - i);
				if (n < 0) {
					if (this.i > this.offset)
						throw new InputMismatchException("colfer: pending data with EOF");
					return null;
				}
				assert n > 0;
				i += n;
			}
		}

	}


	/**
	 * Serializes the object.
	 * @param out the data destination.
	 * @param buf the initial buffer or {@code null}.
	 * @return the final buffer. When the serial fits into {@code buf} then the return is {@code buf}.
	 *  Otherwise the return is a new buffer, large enough to hold the whole serial.
	 * @throws IOException from {@code out}.
	 * @throws IllegalStateException on an upper limit breach defined by {@link #colferSizeMax}.
	 */
	public byte[] marshal(OutputStream out, byte[] buf) throws IOException {
		// TODO: better size estimation
		if (buf == null || buf.length == 0)
			buf = new byte[Math.min(TxnCacheKey.colferSizeMax, 2048)];

		while (true) {
			int i;
			try {
				i = marshal(buf, 0);
			} catch (BufferOverflowException e) {
				buf = new byte[Math.min(TxnCacheKey.colferSizeMax, buf.length * 4)];
				continue;
			}

			out.write(buf, 0, i);
			return buf;
		}
	}

	/**
	 * Serializes the object.
	 * @param buf the data destination.
	 * @param offset the initial index for {@code buf}, inclusive.
	 * @return the final index for {@code buf}, exclusive.
	 * @throws BufferOverflowException when {@code buf} is too small.
	 * @throws IllegalStateException on an upper limit breach defined by {@link #colferSizeMax}.
	 */
	public int marshal(byte[] buf, int offset) {
		int i = offset;

		try {
			if (! this.transactionId.isEmpty()) {
				buf[i++] = (byte) 0;
				int start = ++i;

				String s = this.transactionId;
				for (int sIndex = 0, sLength = s.length(); sIndex < sLength; sIndex++) {
					char c = s.charAt(sIndex);
					if (c < '\u0080') {
						buf[i++] = (byte) c;
					} else if (c < '\u0800') {
						buf[i++] = (byte) (192 | c >>> 6);
						buf[i++] = (byte) (128 | c & 63);
					} else if (c < '\ud800' || c > '\udfff') {
						buf[i++] = (byte) (224 | c >>> 12);
						buf[i++] = (byte) (128 | c >>> 6 & 63);
						buf[i++] = (byte) (128 | c & 63);
					} else {
						int cp = 0;
						if (++sIndex < sLength) cp = Character.toCodePoint(c, s.charAt(sIndex));
						if ((cp >= 1 << 16) && (cp < 1 << 21)) {
							buf[i++] = (byte) (240 | cp >>> 18);
							buf[i++] = (byte) (128 | cp >>> 12 & 63);
							buf[i++] = (byte) (128 | cp >>> 6 & 63);
							buf[i++] = (byte) (128 | cp & 63);
						} else
							buf[i++] = (byte) '?';
					}
				}
				int size = i - start;
				if (size > TxnCacheKey.colferSizeMax)
					throw new IllegalStateException(format("colfer: com/r2l/model/common/colf.TxnCacheKey.transactionId size %d exceeds %d UTF-8 bytes", size, TxnCacheKey.colferSizeMax));

				int ii = start - 1;
				if (size > 0x7f) {
					i++;
					for (int x = size; x >= 1 << 14; x >>>= 7) i++;
					System.arraycopy(buf, start, buf, i - size, size);

					do {
						buf[ii++] = (byte) (size | 0x80);
						size >>>= 7;
					} while (size > 0x7f);
				}
				buf[ii] = (byte) size;
			}

			if (this.serialNo != 0) {
				int x = this.serialNo;
				if ((x & ~((1 << 21) - 1)) != 0) {
					buf[i++] = (byte) (1 | 0x80);
					buf[i++] = (byte) (x >>> 24);
					buf[i++] = (byte) (x >>> 16);
					buf[i++] = (byte) (x >>> 8);
				} else {
					buf[i++] = (byte) 1;
					while (x > 0x7f) {
						buf[i++] = (byte) (x | 0x80);
						x >>>= 7;
					}
				}
				buf[i++] = (byte) x;
			}

			buf[i++] = (byte) 0x7f;
			return i;
		} catch (ArrayIndexOutOfBoundsException e) {
			if (i - offset > TxnCacheKey.colferSizeMax)
				throw new IllegalStateException(format("colfer: com/r2l/model/common/colf.TxnCacheKey exceeds %d bytes", TxnCacheKey.colferSizeMax));
			if (i > buf.length) throw new BufferOverflowException();
			throw e;
		}
	}

	/**
	 * Deserializes the object.
	 * @param buf the data source.
	 * @param offset the initial index for {@code buf}, inclusive.
	 * @return the final index for {@code buf}, exclusive.
	 * @throws BufferUnderflowException when {@code buf} is incomplete. (EOF)
	 * @throws SecurityException on an upper limit breach defined by {@link #colferSizeMax}.
	 * @throws InputMismatchException when the data does not match this object's schema.
	 */
	public int unmarshal(byte[] buf, int offset) {
		return unmarshal(buf, offset, buf.length);
	}

	/**
	 * Deserializes the object.
	 * @param buf the data source.
	 * @param offset the initial index for {@code buf}, inclusive.
	 * @param end the index limit for {@code buf}, exclusive.
	 * @return the final index for {@code buf}, exclusive.
	 * @throws BufferUnderflowException when {@code buf} is incomplete. (EOF)
	 * @throws SecurityException on an upper limit breach defined by {@link #colferSizeMax}.
	 * @throws InputMismatchException when the data does not match this object's schema.
	 */
	public int unmarshal(byte[] buf, int offset, int end) {
		if (end > buf.length) end = buf.length;
		int i = offset;

		try {
			byte header = buf[i++];

			if (header == (byte) 0) {
				int size = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					size |= (b & 0x7f) << shift;
					if (shift == 28 || b >= 0) break;
				}
				if (size < 0 || size > TxnCacheKey.colferSizeMax)
					throw new SecurityException(format("colfer: com/r2l/model/common/colf.TxnCacheKey.transactionId size %d exceeds %d UTF-8 bytes", size, TxnCacheKey.colferSizeMax));

				int start = i;
				i += size;
				this.transactionId = new String(buf, start, size, StandardCharsets.UTF_8);
				header = buf[i++];
			}

			if (header == (byte) 1) {
				int x = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					x |= (b & 0x7f) << shift;
					if (shift == 28 || b >= 0) break;
				}
				this.serialNo = x;
				header = buf[i++];
			} else if (header == (byte) (1 | 0x80)) {
				this.serialNo = (buf[i++] & 0xff) << 24 | (buf[i++] & 0xff) << 16 | (buf[i++] & 0xff) << 8 | (buf[i++] & 0xff);
				header = buf[i++];
			}

			if (header != (byte) 0x7f)
				throw new InputMismatchException(format("colfer: unknown header at byte %d", i - 1));
		} finally {
			if (i > end && end - offset < TxnCacheKey.colferSizeMax) throw new BufferUnderflowException();
			if (i < 0 || i - offset > TxnCacheKey.colferSizeMax)
				throw new SecurityException(format("colfer: com/r2l/model/common/colf.TxnCacheKey exceeds %d bytes", TxnCacheKey.colferSizeMax));
			if (i > end) throw new BufferUnderflowException();
		}

		return i;
	}

	// {@link Serializable} version number.
	private static final long serialVersionUID = 2L;

	// {@link Serializable} Colfer extension.
	private void writeObject(ObjectOutputStream out) throws IOException {
		// TODO: better size estimation
		byte[] buf = new byte[1024];
		int n;
		while (true) try {
			n = marshal(buf, 0);
			break;
		} catch (BufferUnderflowException e) {
			buf = new byte[4 * buf.length];
		}

		out.writeInt(n);
		out.write(buf, 0, n);
	}

	// {@link Serializable} Colfer extension.
	private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
		init();

		int n = in.readInt();
		byte[] buf = new byte[n];
		in.readFully(buf);
		unmarshal(buf, 0);
	}

	// {@link Serializable} Colfer extension.
	private void readObjectNoData() throws ObjectStreamException {
		init();
	}

	/**
	 * Gets com/r2l/model/common/colf.TxnCacheKey.transactionId.
	 * @return the value.
	 */
	public String getTransactionId() {
		return this.transactionId;
	}

	/**
	 * Sets com/r2l/model/common/colf.TxnCacheKey.transactionId.
	 * @param value the replacement.
	 */
	public void setTransactionId(String value) {
		this.transactionId = value;
	}

	/**
	 * Sets com/r2l/model/common/colf.TxnCacheKey.transactionId.
	 * @param value the replacement.
	 * @return {link this}.
	 */
	public TxnCacheKey withTransactionId(String value) {
		this.transactionId = value;
		return this;
	}

	/**
	 * Gets com/r2l/model/common/colf.TxnCacheKey.serialNo.
	 * @return the value.
	 */
	public int getSerialNo() {
		return this.serialNo;
	}

	/**
	 * Sets com/r2l/model/common/colf.TxnCacheKey.serialNo.
	 * @param value the replacement.
	 */
	public void setSerialNo(int value) {
		this.serialNo = value;
	}

	/**
	 * Sets com/r2l/model/common/colf.TxnCacheKey.serialNo.
	 * @param value the replacement.
	 * @return {link this}.
	 */
	public TxnCacheKey withSerialNo(int value) {
		this.serialNo = value;
		return this;
	}

	@Override
	public final int hashCode() {
		int h = 1;
		if (this.transactionId != null) h = 31 * h + this.transactionId.hashCode();
		h = 31 * h + this.serialNo;
		return h;
	}

	@Override
	public final boolean equals(Object o) {
		return o instanceof TxnCacheKey && equals((TxnCacheKey) o);
	}

	public final boolean equals(TxnCacheKey o) {
		if (o == null) return false;
		if (o == this) return true;
		return o.getClass() == TxnCacheKey.class
			&& (this.transactionId == null ? o.transactionId == null : this.transactionId.equals(o.transactionId))
			&& this.serialNo == o.serialNo;
	}

}
