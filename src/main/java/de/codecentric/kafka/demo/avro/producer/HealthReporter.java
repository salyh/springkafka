package de.codecentric.kafka.demo.avro.producer;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

public class HealthReporter implements MetricsReporter {

	private final Notifier notifier = new SysoutNotifier();
	private final Map<String, Evaluator> registeredMetrics = new HashMap<>();
	private final AtomicBoolean running = new AtomicBoolean(true);
	private final Set<KafkaMetric> metrics = new HashSet<>();
	private final Dumper dumper = new Dumper();
	private final long interval = 3000; //ms
	
	@Override
	public void configure(Map<String, ?> configs) {
		registerMetric("connection-count", "producer-metrics", new SimpleCurrentRangeEvaluator(1, 10));
		registerMetric("record-error-rate", "producer-metrics", new SimpleCurrentRangeEvaluator(0, 0));
		registerMetric("request-latency-avg", "producer-metrics", new SimpleCurrentRangeEvaluator(0, 20));
		registerMetric("waiting-threads", "producer-metrics", new SimpleCurrentRangeEvaluator(0, 5));
		registerMetric("record-retry-rate", "producer-metrics", new SimpleCurrentRangeEvaluator(0, 0));
		registerMetric("no-such-metric", "dummy", new SimpleCurrentRangeEvaluator(0, 0));
	}

	@Override
	public void init(List<KafkaMetric> metrics) {
		metrics.addAll(metrics);
		dumper.start();
	}

	@Override
	public void metricChange(KafkaMetric metric) {
		metrics.add(metric);
	}

	@Override
	public void metricRemoval(KafkaMetric metric) {
		metrics.remove(metric);
	}

	@Override
	public void close() {
		running.set(false);
		dumper.interrupt();
	}
	
	private void registerMetric(String metricName, String metricGroup, Evaluator evaluator) {
		registeredMetrics.put(metricName+metricGroup, evaluator);
	}
	
	private class Dumper extends Thread {

		@Override
		public void run() {
			
			outer:
			while(running.get()) {
				
				
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					break outer;
				}
				
				System.out.println("---- "+new Date()+" ----");
				Set<String> registeredMetricNames = registeredMetrics.keySet();
				for(KafkaMetric metric: new HashSet<>(metrics)) {
					Evaluator evaluator = registeredMetrics.get(metric.metricName().name()+metric.metricName().group());
					
					Optional<EvaluationResult> result;
					if(evaluator != null) {
						if((result = evaluator.evaluate(metric)).isPresent()) {
							try {
								notifier.sendNotification(metric, result.get());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					
						registeredMetricNames.remove(metric.metricName().name()+metric.metricName().group());
					} 
					
				}
				
				
				if(!registeredMetricNames.isEmpty()) {
					System.out.println("No such metrics: "+registeredMetricNames);
				}
				
				
			}
			
		}
		
	}
	
	
    private static interface EvaluationResult {
		
		public String getExplanation(KafkaMetric metric) ;
	}
    
    private static class SimpleCurrentRangeEvaluationResult implements EvaluationResult { 
		
    	private final double min;
		private final double max;
    	
		public SimpleCurrentRangeEvaluationResult(double min, double max) {
			super();
			this.min = min;
			this.max = max;
		}


		public String getExplanation(KafkaMetric metric) {
			return "Metric '"+metric.metricName().name()+"' has a value of "+metric.value()+" which is not within the allowed range of "+min+"-"+max;
		}
	}
	
	private static interface Evaluator {
		Optional<EvaluationResult> evaluate(KafkaMetric metric);
	}
	
	private static class SimpleCurrentRangeEvaluator implements Evaluator {

		private final double min;
		private final double max;

		public SimpleCurrentRangeEvaluator(double min, double max) {
			super();
			this.min = min;
			this.max = max;
		}

		@Override
		public Optional<EvaluationResult> evaluate(KafkaMetric metric) {
			if(metric.value() < min || metric.value() > max) {
				return Optional.of(new SimpleCurrentRangeEvaluationResult(min, max));
			}
			
			return Optional.empty();
		}
		
	}
	
	private static interface Notifier {
		boolean sendNotification(KafkaMetric metric, EvaluationResult result) throws IOException;
	}

	private class SysoutNotifier implements Notifier {

		@Override
		public boolean sendNotification(KafkaMetric metric, EvaluationResult result) throws IOException {
			System.out.println(result.getExplanation(metric));
			return true;
		}
		
	}
}
