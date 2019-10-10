package io.github.zomky.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public class MicrometerMetricsCollector implements MetricsCollector {

    private final Counter requests;

    public MicrometerMetricsCollector(MeterRegistry registry) {
        requests = registry.counter("requests");
    }

    @Override
    public void incrementRequests() {
        requests.increment();
    }
}
