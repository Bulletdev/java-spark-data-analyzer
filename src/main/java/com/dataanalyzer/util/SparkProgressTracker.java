package com.dataanalyzer.util;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Listens to Spark scheduler events and renders a real-time progress
 * bar to stdout while jobs are running.
 *
 * <p>Register once via
 * {@code SparkSession.sparkContext().addSparkListener(tracker)}.
 * The bar is printed on a daemon thread so it never blocks the
 * Spark event loop.
 */
public class SparkProgressTracker extends SparkListener {

    /** Refresh interval for the display thread in milliseconds. */
    private static final int REFRESH_MS = 150;

    /** Width of the printed progress bar in characters. */
    private static final int BAR_WIDTH = 30;

    /** Characters used to clear the progress line. */
    private static final int CLEAR_WIDTH = 70;

    /** Number of spinner frames. */
    private static final int SPINNER_FRAMES = 4;

    /** Number of active (not yet finished) Spark jobs. */
    private final AtomicInteger activeJobs = new AtomicInteger(0);

    /** Number of tasks that have completed across current jobs. */
    private final AtomicInteger completedTasks = new AtomicInteger(0);

    /** Total tasks submitted across current jobs. */
    private final AtomicInteger totalTasks = new AtomicInteger(0);

    /** Background thread that updates the terminal display. */
    private volatile Thread displayThread;

    /** {@inheritDoc} */
    @Override
    public void onJobStart(final SparkListenerJobStart jobStart) {
        if (activeJobs.getAndIncrement() == 0) {
            completedTasks.set(0);
            totalTasks.set(0);
            startDisplay();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onJobEnd(final SparkListenerJobEnd jobEnd) {
        if (activeJobs.decrementAndGet() == 0) {
            stopDisplay();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onStageSubmitted(
            final SparkListenerStageSubmitted stageSubmitted) {
        totalTasks.addAndGet(stageSubmitted.stageInfo().numTasks());
    }

    /** {@inheritDoc} */
    @Override
    public void onTaskEnd(final SparkListenerTaskEnd taskEnd) {
        completedTasks.incrementAndGet();
    }

    /** Starts the daemon thread that redraws the progress bar. */
    private void startDisplay() {
        ProgressBar bar = new ProgressBar(BAR_WIDTH, '\u2588', '\u2591', true);
        String[] spinner = {"|", "/", "-", "\\"};
        displayThread = new Thread(() -> {
            int tick = 0;
            while (!Thread.currentThread().isInterrupted()) {
                int done = completedTasks.get();
                int total = totalTasks.get();
                if (total > 0) {
                    bar.update((double) done / total, "processando...");
                } else {
                    System.out.printf(
                        "\r  %s processando...",
                        spinner[tick % SPINNER_FRAMES]);
                    tick++;
                }
                System.out.flush();
                try {
                    Thread.sleep(REFRESH_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        displayThread.setDaemon(true);
        displayThread.start();
    }

    /** Stops the display thread and clears the progress line. */
    private void stopDisplay() {
        Thread t = displayThread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(REFRESH_MS * 2);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.printf("\r%s\r", " ".repeat(CLEAR_WIDTH));
        System.out.flush();
    }
}
