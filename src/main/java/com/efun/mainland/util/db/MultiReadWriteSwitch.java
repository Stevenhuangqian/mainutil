package com.efun.mainland.util.db;

import java.util.HashMap;
import java.util.Map;

public class MultiReadWriteSwitch {
	protected Map<String, SingleReadWriteSwitch> multiReadWriteSwitchMap = new HashMap<String, SingleReadWriteSwitch>();

	public MultiReadWriteSwitch() {

	}

	public MultiReadWriteSwitch(Map<String, SingleReadWriteSwitch> multiReadWriteSwitchMap) {
		if (multiReadWriteSwitchMap != null) {
			this.multiReadWriteSwitchMap = multiReadWriteSwitchMap;
		}
	}

	public Map<String, SingleReadWriteSwitch> getMultiReadWriteSwitchMap() {
		return multiReadWriteSwitchMap;
	}

	public void setMultiReadWriteSwitchMap(Map<String, SingleReadWriteSwitch> multiReadWriteSwitchMap) {
		if (multiReadWriteSwitchMap != null) {
			this.multiReadWriteSwitchMap = multiReadWriteSwitchMap;
		}
	}

}
