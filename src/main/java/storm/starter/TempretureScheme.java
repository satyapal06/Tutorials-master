package storm.starter;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TempretureScheme implements Scheme {

	public static final String FIELD_TEMPERATURE_ID = "temperatureId";
	public static final String FIELD_EVENT_TIME = "eventTime";
	public static final String FIELD_EVENT_TYPE = "eventType";

	public static final String FIELD_INCIDENT_CNT = "incidentCnt";

	private static final long serialVersionUID = -2990121166902741545L;

	private static final Logger LOG = Logger.getLogger(TempretureScheme.class);

	@Override
	public List<Object> deserialize(byte[] bytes) {
		try {
			String truckEvent = new String(bytes, "UTF-8");
			String[] pieces = truckEvent.split("\\|");

			Timestamp eventTime = Timestamp.valueOf(pieces[0]);
			String temperatureId = pieces[1];
			String eventType = pieces[2];
			return new Values(cleanup(temperatureId), cleanup(eventType), eventTime);
		} catch (UnsupportedEncodingException e) {
			LOG.error(e);
			throw new RuntimeException(e);
		}

	}

	@Override
	public Fields getOutputFields() {
		return new Fields(FIELD_TEMPERATURE_ID, FIELD_EVENT_TIME, FIELD_EVENT_TYPE);
	}

	private String cleanup(String str) {
		if (str != null) {
			return str.trim().replace("\n", "").replace("\t", "");
		} else {
			return str;
		}
	}
}