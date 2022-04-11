package org.forome.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DurationUtils {

    public static String toString(Duration duration) {
        long durationMillis = duration.toMillis();
        long durationMin = TimeUnit.MILLISECONDS.toSeconds(durationMillis) / TimeUnit.MINUTES.toSeconds(1);
        long durationSec = durationMillis / TimeUnit.SECONDS.toMillis(1);
        long durationMs = durationMillis % TimeUnit.SECONDS.toMillis(1);
        if (durationMin != 0) {
            durationSec -= (durationMin * TimeUnit.MINUTES.toSeconds(1));
            return String.format("%d m %d s %d ms", durationMin, durationSec, durationMs);
        } else if (durationSec != 0) {
            return String.format("%d s %d ms", durationSec, durationMs);
        }

        return String.format("%d ms", durationMs);
    }
}
