package de.fzi.cep.sepa.sources.samples.taxi;

import java.io.File;

import org.apache.log4j.Logger;

import de.fzi.cep.sepa.model.vocabulary.MessageFormat;
import de.fzi.cep.sepa.model.impl.EventGrounding;
import de.fzi.cep.sepa.model.impl.EventSchema;
import de.fzi.cep.sepa.model.impl.EventStream;
import de.fzi.cep.sepa.model.impl.TransportFormat;
import de.fzi.cep.sepa.model.impl.graph.SepDescription;
import de.fzi.cep.sepa.sources.samples.config.SampleSettings;
import de.fzi.cep.sepa.sources.samples.config.SourcesConfig;

public class NYCTaxiStream extends AbstractNycStream {
	
	public static final Logger logger = Logger.getLogger(NYCTaxiStream.class);

	public NYCTaxiStream() {
		super(NycSettings.sampleTopic);
	}
	
	@Override
	public EventStream declareModel(SepDescription sep) {
			
		EventStream stream = new EventStream();
		stream.setIconUrl(SourcesConfig.iconBaseUrl + "/Taxi_Icon_2" +"_HQ.png");
		EventSchema schema = NycTaxiUtils.getEventSchema();

		EventGrounding grounding = new EventGrounding();
		grounding.setTransportProtocol(SampleSettings.kafkaProtocol(NycSettings.sampleTopic));
		grounding.setTransportFormats(de.fzi.cep.sepa.commons.Utils.createList(new TransportFormat(MessageFormat.Json)));
		
		stream.setEventGrounding(grounding);
		stream.setEventSchema(schema);
		stream.setName("NYC Taxi Sample Stream");
		stream.setDescription("NYC Taxi Sample Stream Description");
		stream.setUri(sep.getUri() + "/sample");

		return stream;
	}

	@Override
	public void executeStream() {	
		File file = new File(NycSettings.completeDatasetFilename);
		executeReplay(file);
	}

	@Override
	public boolean isExecutable() {
		return true;
	}
	

	/**
	 * Sending
	 */
	class OutputThread implements Runnable {
		long diff;

		public OutputThread(long sleepTime) {
			diff = sleepTime;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(diff);
				synchronized (publisher) {
					//publisher.sendText(json);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

}
