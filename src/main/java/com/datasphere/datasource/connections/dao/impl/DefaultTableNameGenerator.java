package com.datasphere.datasource.connections.dao.impl;

import com.datasphere.datasource.connections.dao.TableNameGenerator;

public class DefaultTableNameGenerator implements TableNameGenerator {
	public static final String PREFIX = "DATASET_";
	
	@Override
	public String generate(String factor) {
		return PREFIX + factor;
	}
}
