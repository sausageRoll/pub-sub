package com.pubsub.broker;

import java.util.concurrent.TimeUnit;

/** A {@link RelativeTimeProvider} that uses {@link System#nanoTime}. */
class SystemRelativeTimeProvider implements RelativeTimeProvider {
    @Override
    public long relativeTime(TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    }
}
