package org.jocean.opentracing;

import java.util.concurrent.TimeUnit;

public interface DurationRecorder {
    public void record(final long amount, final TimeUnit unit, final String...tags);

    static DurationRecorder _NoopRecorder = new DurationRecorder() {
        @Override
        public void record(final long amount, final TimeUnit unit, final String... tags) {
        }};
}
