package storm.kafka.producer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

public class TemperatureEventProducer {
	private static final Logger LOG = Logger.getLogger(TemperatureEventProducer.class);

	public static void main(String[] args) throws ParserConfigurationException,
			SAXException, IOException, URISyntaxException {
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("zk.connect", "localhost:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		String TOPIC = "temperatureevent";
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		String[] events = { "Normal", "Exceed", "Average" };
		int evtCnt = events.length;

		String finalEvent = "";
		Random random = new Random();

		for (int i = 0; i < 10000000; i++) {

			finalEvent = new Timestamp(new Date().getTime()) + "|"
					+ random.nextInt(45) + "|" + events[random.nextInt(evtCnt)];
			try {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
				LOG.info("Sending Messge #: " + i + ", msg:" + finalEvent);
				producer.send(data);
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}