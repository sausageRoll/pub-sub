package com.pubsub.broker;

import java.util.concurrent.TimeUnit;

/** Interface for classes that can provide a relative time stamp. */
interface RelativeTimeProvider {
    /** Returns a time stamp that can be used for calculating elapsed time. */
    public long relativeTime(TimeUnit timeUnit);
}
