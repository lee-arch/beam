package com.my;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.r2l.model.common.colf.OutboxRecord;

public interface RecordReflector {
	String eventTarget();

	PreparedStatement createUpdateStatement(Connection connection, OutboxRecord record);
}
