package com.dataanalyzer.ui;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Wraps {@link Scanner} with typed, prompt-aware read methods.
 *
 * <p>Isolating input reading here makes it straightforward to swap
 * in a mock during testing without touching business logic.
 */
public class InputReader {

    /** Underlying scanner used to read user input. */
    private final Scanner scanner;

    /**
     * @param inputScanner the underlying scanner to read from
     */
    public InputReader(final Scanner inputScanner) {
        this.scanner = inputScanner;
    }

    /**
     * Prints a prompt and reads the next trimmed line of input.
     *
     * @param prompt message displayed before reading
     * @return trimmed input string (never {@code null})
     */
    public String readLine(final String prompt) {
        System.out.print(prompt);
        return scanner.nextLine().trim();
    }

    /**
     * Prints a prompt and reads an integer.
     *
     * @param prompt message displayed before reading
     * @return parsed integer, or {@code -1} if the input is not valid
     */
    public int readInt(final String prompt) {
        System.out.print(prompt);
        try {
            return Integer.parseInt(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println(
                "Entrada inválida. Digite um número inteiro.");
            return -1;
        }
    }

    /**
     * Prints a prompt and reads a yes/no answer.
     *
     * @param prompt message displayed before reading
     * @return {@code true} if the answer starts with {@code 's'}
     *         (case-insensitive)
     */
    public boolean readBoolean(final String prompt) {
        System.out.print(prompt);
        return scanner.nextLine().trim().toLowerCase().startsWith("s");
    }

    /**
     * Prints a prompt and reads a comma-separated list of values.
     *
     * @param prompt message displayed before reading
     * @return list of non-empty, trimmed tokens
     */
    public List<String> readList(final String prompt) {
        System.out.print(prompt);
        return Arrays.stream(scanner.nextLine().split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }
}
