package com.efun.mainland.util.db;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

public class SingleReadWriteSwitch {
	protected Object read;
	protected DataSource write;

	public SingleReadWriteSwitch() {

	}

	public SingleReadWriteSwitch(DataSource readAndWrite) {
		this.read = readAndWrite;
		this.write = readAndWrite;
	}

	public SingleReadWriteSwitch(Object read, DataSource write) {
		this.read = read;
		this.write = write;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public DataSource selectRead() {
		if (read == null) {
			return selectWrite();
		} else {
			Object tmp = null;
			if (read instanceof DataSource) {
				return (DataSource) read;
			} else if (read instanceof List) {
				List<Object> temp = (List<Object>) read;
				int size = temp.size();
				if (size == 0) {
					tmp = selectWrite();
				} else if (size > 1) {
					tmp = temp.get(new SecureRandom().nextInt(size));
				} else {
					tmp = temp.get(0);
				}
			} else if (read instanceof Map) {
				Object[] temp = ((Map) read).values().toArray();
				int size = temp.length;
				if (size == 0) {
					tmp = selectWrite();
				} else if (size > 1) {
					tmp = temp[new SecureRandom().nextInt(size)];
				} else {
					tmp = temp[0];
				}
			} else if (read instanceof Set) {
				Object[] temp = ((Set) read).toArray();
				int size = temp.length;
				if (size == 0) {
					tmp = selectWrite();
				} else if (size > 1) {
					tmp = temp[new SecureRandom().nextInt(size)];
				} else {
					tmp = temp[0];
				}
			} else if (read instanceof DataSource[]) {
				DataSource[] temp = (DataSource[]) read;
				int size = temp.length;
				if (size == 0) {
					tmp = selectWrite();
				} else if (size > 1) {
					tmp = temp[new SecureRandom().nextInt(size)];
				} else {
					tmp = temp[0];
				}
			}
			if (tmp != null && (tmp instanceof DataSource)) {
				return (DataSource) tmp;
			} else {
				throw new IllegalArgumentException("read Type error:javax.sql.DataSource");
			}
		}
	}

	public DataSource selectWrite() {
		if (write != null) {
			return write;
		} else {
			throw new IllegalArgumentException("write Type error:javax.sql.DataSource");
		}
	}

	public void setRead(Object read) {
		this.read = read;
	}

	public void setWrite(DataSource write) {
		this.write = write;
	}

	public Object getRead() {
		return read;
	}

	public DataSource getWrite() {
		return write;
	}
}
