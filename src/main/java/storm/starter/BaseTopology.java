package storm.starter;

import java.util.Properties;

public abstract class BaseTopology {
	protected Properties topologyConfig;

	public BaseTopology() throws Exception {
		topologyConfig = new Properties();
		topologyConfig.put("kafka.zookeeper.host.port", "23.253.230.238");
		topologyConfig.put("kafka.topic", "temperatureevent");
		topologyConfig.put("kafka.zkRoot", "/temterature_event_sprout");
		topologyConfig.put("kafka.groupid", "test-consumer-group");
		topologyConfig.put("spout.thread.count", 1);
	}
}