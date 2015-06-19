package storm.kafka.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import storm.starter.TempretureScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class KafkaBolt extends BaseRichBolt {
	private static final long serialVersionUID = 8998534364225858062L;
	private static final Logger LOG = Logger.getLogger(KafkaBolt.class);

	public void declareOutputFields(OutputFieldsDeclarer ofd) { }

	public void prepare(Map map, TopologyContext tc, OutputCollector oc) { }

	public void execute(Tuple tuple) {
		LOG.info(tuple.getValueByField(TempretureScheme.FIELD_EVENT_TIME)  + "," +
          tuple.getStringByField(TempretureScheme.FIELD_EVENT_TYPE)  + "," +
          tuple.getStringByField(TempretureScheme.FIELD_TEMPERATURE_ID));
	}
}