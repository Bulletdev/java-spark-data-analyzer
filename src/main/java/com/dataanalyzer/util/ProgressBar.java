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

    private final int width;
    private final char barChar;
    private final char emptyChar;
    private final String format;
    private String currentStatus = "";

    /**
     * Creates a progress bar with custom settings.
     *
     * @param width       total number of bar characters
     * @param barChar     character used for the filled portion
     * @param emptyChar   character used for the empty portion
     * @param showPercent whether to render a percentage value
     */
    public ProgressBar(int width, char barChar, char emptyChar,
                       boolean showPercent) {
        this.width = width;
        this.barChar = barChar;
        this.emptyChar = emptyChar;
        this.format = showPercent ? "\r[%s] %3d%% %s" : "\r[%s] %s";
    }

    /** Creates a default 50-character bar with percentage display. */
    public ProgressBar() {
        this(50, '█', '░', true);
    }

    /**
     * Updates the progress bar.
     *
     * @param progress value between 0.0 and 1.0 (clamped automatically)
     * @param status   status message shown after the bar
     */
    public void update(double progress, String status) {
        double clamped = Math.max(0.0, Math.min(1.0, progress));
        int filledWidth = (int) (width * clamped);

        StringBuilder bar = new StringBuilder();
        for (int i = 0; i < width; i++) {
            bar.append(i < filledWidth ? barChar : emptyChar);
        }

        currentStatus = status != null ? status : "";

        if (format.contains("%3d")) {
            System.out.printf(
                format, bar.toString(), (int) (clamped * 100), currentStatus);
        } else {
            System.out.printf(format, bar.toString(), currentStatus);
        }
    }

    /**
     * Updates the progress bar keeping the previous status message.
     *
     * @param progress value between 0.0 and 1.0
     */
    public void update(double progress) {
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
    public void complete(String completionMessage) {
        System.out.println();
        System.out.println(completionMessage);
    }
}
