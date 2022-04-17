package com.github.laz2.utils;

import com.google.common.base.CaseFormat;
import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@UtilityClass
public class ThreadUtils {

    public static String getThreadName(Object... parts) {
        return Arrays.stream(parts)
            .map(obj -> {
                String name;
                if (obj instanceof Class) {
                    name = ((Class<?>) obj).getSimpleName();
                } else {
                    name = String.valueOf(obj);
                }
                return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, name);
            })
            .collect(Collectors.joining("-"));
    }

    public static boolean sleep(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    public static boolean waitFor(long durationMs, Supplier<Boolean> predicate) {
        var endTime = System.currentTimeMillis() + durationMs;
        while (!predicate.get() && System.currentTimeMillis() < endTime) {
            sleep(200);
        }
        return System.currentTimeMillis() < endTime;
    }

    public static int getNameIndex(String name) {
        var comps = name.split("-");
        return Integer.parseInt(comps[comps.length - 1]);
    }

    public static int getCurrentNameIndex() {
        return getNameIndex(Thread.currentThread().getName());
    }

    public static void startDaemonThread(Runnable target, String name) {
        var t = new Thread(target);
        t.setName(name);
        t.setDaemon(true);
        t.start();
    }
}
