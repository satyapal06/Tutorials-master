package storm.starter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.hortonworks.tutorials.tutorial2.BaseTruckEventTopology;

public abstract class BaseTopology {
	private static final Logger LOG = Logger.getLogger(BaseTruckEventTopology.class);
	protected Properties topologyConfig;

	public BaseTopology(String configFileLocation) throws Exception {
		try {
			topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
		} catch (FileNotFoundException e) {
			LOG.error("Encountered error while reading configuration properties: " + e.getMessage());
			throw e;
		} catch (IOException e) {
			LOG.error("Encountered error while reading configuration properties: " + e.getMessage());
			throw e;
		}
	}
}
