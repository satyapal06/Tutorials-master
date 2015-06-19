package storm.starter;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TemperatureRedisBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7808515437215417696L;
	private final String redisHost;
	private final int redisPort;
	private Jedis redis;

	public TemperatureRedisBolt(String redisHost, int redisPort) {
		this.redisHost = redisHost;
		this.redisPort = redisPort;
	}

	public void execute(Tuple tuple) {
		loadData(tuple);
	}

	public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
		redis = new Jedis(redisHost, redisPort);
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {

	}

	public void loadData(Tuple tuple) {
		Object eventTime = tuple.getValueByField(TempretureScheme.FIELD_EVENT_TIME);
		String temperatureId = tuple.getStringByField(TempretureScheme.FIELD_TEMPERATURE_ID);
		String eventType = tuple.getStringByField(TempretureScheme.FIELD_EVENT_TYPE);
		
		String temperatureList = eventTime  + "," + temperatureId  + "," + eventType;
		redis.lpush("temperature-list", temperatureList);
	}
}
