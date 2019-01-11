package org.jocean.opentracing;

import io.opentracing.Tracer;

public class TracingUtil {
    final static ThreadLocal<Tracer> tlsTracer = new ThreadLocal<>();

    public static void set(final Tracer tracer) {
        tlsTracer.set(tracer);
    }

    public static Tracer get() {
        return tlsTracer.get();
    }
}
