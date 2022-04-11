package org.forome.rocksdb.util;

import org.forome.util.DurationUtils;

import javax.swing.*;
import java.time.Duration;

public class PerfomanceTest {

    @FunctionalInterface
    public interface Action {

        void process(int step) throws Exception;
    }

    @FunctionalInterface
    public interface Consumer {

        void accept() throws Exception;
    }

    public static void test(int executionCount, Action action) throws Exception {
        test(executionCount, null, action);
    }

    public static void test(int executionCount, Consumer beforeEach, Action action) throws Exception {
        showMessage("Test on starting.");

        long durationNanos = 0;
        for (int i = 0; i < executionCount; ++i) {
            if (beforeEach != null) {
                beforeEach.accept();
            }
            long beginTime = System.nanoTime();
            action.process(i);
            durationNanos += (System.nanoTime() - beginTime);
        }
        printResults(executionCount, Duration.ofNanos(durationNanos));

        showMessage("Test on finished.");
    }

    private static void printResults(int executionCount, Duration duration) {
        String msg = String.format("Total execution count = %d, Total time = %s, Time spent on one call = %s",
                executionCount,
                DurationUtils.toString(duration),
                DurationUtils.toString(duration.dividedBy(executionCount))
        );
        System.out.println(msg);
    }

    private static void showMessage(String message) {
        JOptionPane optionPane = new JOptionPane(message);
        JDialog dialog = optionPane.createDialog("Perfomance test");
        dialog.setAlwaysOnTop(true);
        dialog.setVisible(true);
    }
}
