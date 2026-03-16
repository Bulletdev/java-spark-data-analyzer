package com.dataanalyzer.util;

/**
 * Renders a simple in-place terminal progress bar.
 *
 * <p>Uses carriage return ({@code \r}) to overwrite the current line.
 * Not thread-safe — intended for single-threaded use only.
 *
 * <pre>
 *   ProgressBar bar = new ProgressBar();
 *   bar.update(0.5, "Processing...");
 *   bar.complete("Done!");
 * </pre>
 */
public class ProgressBar {

    /** Default width (number of bar characters) for the no-arg constructor. */
    private static final int DEFAULT_WIDTH = 50;

    /** Multiplier used to convert a [0,1] progress fraction to a percentage. */
    private static final int PERCENT_FACTOR = 100;

    /** Total number of bar characters rendered inside the brackets. */
    private final int width;

    /** Character used for the filled portion of the bar. */
    private final char barChar;

    /** Character used for the empty portion of the bar. */
    private final char emptyChar;

    /** Printf format string, chosen at construction time. */
    private final String format;

    /** Most recently set status message, kept for single-arg update calls. */
    private String currentStatus = "";

    /**
     * Creates a progress bar with custom settings.
     *
     * @param barWidth    total number of bar characters
     * @param filledChar  character used for the filled portion
     * @param emptyCharIn character used for the empty portion
     * @param showPercent whether to render a percentage value
     */
    public ProgressBar(final int barWidth, final char filledChar,
                       final char emptyCharIn, final boolean showPercent) {
        this.width = barWidth;
        this.barChar = filledChar;
        this.emptyChar = emptyCharIn;
        this.format = showPercent ? "\r[%s] %3d%% %s" : "\r[%s] %s";
    }

    /** Creates a default 50-character bar with percentage display. */
    public ProgressBar() {
        this(DEFAULT_WIDTH, '\u2588', '\u2591', true);
    }

    /**
     * Updates the progress bar.
     *
     * @param progress value between 0.0 and 1.0 (clamped automatically)
     * @param status   status message shown after the bar
     */
    public void update(final double progress, final String status) {
        double clamped = Math.max(0.0, Math.min(1.0, progress));
        int filledWidth = (int) (width * clamped);

        StringBuilder bar = new StringBuilder();
        for (int i = 0; i < width; i++) {
            bar.append(i < filledWidth ? barChar : emptyChar);
        }

        currentStatus = status != null ? status : "";

        if (format.contains("%3d")) {
            System.out.printf(
                format, bar.toString(),
                (int) (clamped * PERCENT_FACTOR), currentStatus);
        } else {
            System.out.printf(format, bar.toString(), currentStatus);
        }
    }

    /**
     * Updates the progress bar keeping the previous status message.
     *
     * @param progress value between 0.0 and 1.0
     */
    public void update(final double progress) {
        update(progress, currentStatus);
    }

    /** Moves the cursor to a new line, marking the bar as finished. */
    public void complete() {
        System.out.println();
    }

    /**
     * Moves the cursor to a new line and prints a completion message.
     *
     * @param completionMessage message to display after finishing
     */
    public void complete(final String completionMessage) {
        System.out.println();
        System.out.println(completionMessage);
    }
}
