package com.dataanalyzer.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Records the sequence of operations applied to the current DataFrame
 * during an interactive session.
 *
 * <p>Each entry is timestamped to the second and includes both a short
 * operation label and an optional details string.
 */
public class OperationHistory {

    /** Chronological list of recorded operation entries. */
    private final List<String> entries = new ArrayList<>();

    /**
     * Appends a new entry to the history.
     *
     * @param operation short operation name (e.g. {@code "Filter"})
     * @param details   human-readable description of the parameters used
     */
    public void record(final String operation, final String details) {
        String timestamp = LocalDateTime.now()
            .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        entries.add(String.format(
            "[%s] %s \u2014 %s", timestamp, operation, details));
    }

    /**
     * Prints all recorded entries to stdout.
     *
     * <p>If no operations have been recorded yet, prints a notice message
     * instead of an empty block.
     */
    public void print() {
        if (entries.isEmpty()) {
            System.out.println("Nenhuma operação registrada.");
            return;
        }
        System.out.println("\n--- Histórico de Operações ---");
        entries.forEach(System.out::println);
    }

    /**
     * Removes all recorded entries.
     */
    public void clear() {
        entries.clear();
    }

    /**
     * Returns all entries concatenated by newlines, suitable for file export.
     *
     * @return multi-line string of history entries, or empty string if none
     */
    public String toText() {
        return String.join("\n", entries);
    }

    /**
     * Returns an unmodifiable view of the history entries.
     *
     * @return unmodifiable list of history strings
     */
    public List<String> getEntries() {
        return Collections.unmodifiableList(entries);
    }
}
