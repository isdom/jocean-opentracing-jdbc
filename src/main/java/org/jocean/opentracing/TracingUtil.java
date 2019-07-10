package org.jocean.opentracing;

import io.opentracing.Tracer;

public class TracingUtil {
    final static ThreadLocal<Tracer> tlsTracer = new ThreadLocal<>();
    final static ThreadLocal<DurationRecorder> tlsDurationRecorder = new ThreadLocal<>();

    public static void set(final Tracer tracer) {
        tlsTracer.set(tracer);
    }

    public static Tracer get() {
        return tlsTracer.get();
    }

    public static void setDurationRecorder(final DurationRecorder recorder) {
        tlsDurationRecorder.set(recorder);
    }

    public static DurationRecorder getDurationRecorder() {
//        final DurationRecorder recorder = tlsDurationRecorder.get();
//        return null != recorder ? recorder : DurationRecorder._NoopRecorder;
        return tlsDurationRecorder.get();
    }
}
