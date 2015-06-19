package storm.starter;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class KafkaTopology extends BaseTopology {
	private static final String KAFKA_SPOUT_ID = "kafkaSpout";
	private static final String LOG_TRUCK_BOLT_ID = "logTruckEventBolt";

	public KafkaTopology(String configFileLocation) throws Exception {
		super(configFileLocation);
	}

	private SpoutConfig constructKafkaSpoutConf() {
		BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
		String topic = topologyConfig.getProperty("kafka.topic");
		String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
		String consumerGroupId = "StormSpout";

		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new TempretureScheme());

		return spoutConfig;
	}

	public void configureKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
		int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
		builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
	}

	public void configureLogTruckEventBolt(TopologyBuilder builder) {
		KafkaBolt kafkaBolt = new KafkaBolt();
		builder.setBolt(LOG_TRUCK_BOLT_ID, kafkaBolt).globalGrouping(KAFKA_SPOUT_ID);
	}

	private void buildAndSubmit() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		configureKafkaSpout(builder);
		configureLogTruckEventBolt(builder);

		Config conf = new Config();
		conf.setDebug(true);

		StormSubmitter.submitTopology("truck-event-processor", conf, builder.createTopology());
	}

	public static void main(String[] str) throws Exception {
		String configFileLocation = "truck_event_topology.properties";
		KafkaTopology truckTopology = new KafkaTopology(configFileLocation);
		truckTopology.buildAndSubmit();
	}
}
