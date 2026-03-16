package com.dataanalyzer.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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

    /** Directories ignored during recursive file search. */
    private static final List<String> SKIP_DIRS =
        Arrays.asList("target", ".git", ".idea", ".mvn");

    /**
     * Lists files with the given extension found recursively,
     * offers a graphical file picker if available, and falls back
     * to manual path input.
     *
     * @param extension file extension to filter (e.g. {@code "csv"})
     * @return chosen file path
     */
    public String readFilePath(final String extension) {
        List<File> files = findFiles(new File("."), extension);

        System.out.println("Como deseja selecionar o arquivo?");
        if (!files.isEmpty()) {
            System.out.println("  [L] Listar arquivos encontrados");
        }
        System.out.println("  [E] Abrir explorador gráfico");
        System.out.println("  [M] Digitar caminho manualmente");
        System.out.print("Escolha: ");

        String mode = scanner.nextLine().trim().toUpperCase();

        if ("L".equals(mode) && !files.isEmpty()) {
            return pickFromList(files);
        }
        if ("E".equals(mode)) {
            String picked = openFileChooser(extension);
            if (picked != null) {
                return picked;
            }
            System.out.println(
                "[AVISO] Explorador não disponível."
                + " Listando arquivos encontrados.");
            if (!files.isEmpty()) {
                return pickFromList(files);
            }
        }

        System.out.print("Caminho do arquivo: ");
        return scanner.nextLine().trim();
    }

    /**
     * Displays a numbered list of files and returns the chosen path.
     *
     * @param files list of files to display
     * @return chosen file path
     */
    private String pickFromList(final List<File> files) {
        System.out.println("Arquivos encontrados:");
        for (int i = 0; i < files.size(); i++) {
            System.out.printf(
                "  [%d] %s%n", i + 1,
                files.get(i).getPath().replaceFirst("^\\./", ""));
        }
        System.out.println("  [0] Digitar caminho manualmente");
        System.out.print("Escolha: ");

        String line = scanner.nextLine().trim();
        try {
            int idx = Integer.parseInt(line);
            if (idx >= 1 && idx <= files.size()) {
                return files.get(idx - 1).getPath();
            }
        } catch (NumberFormatException ignored) {
            // fall through to manual input
        }
        System.out.print("Caminho do arquivo: ");
        return scanner.nextLine().trim();
    }

    /**
     * Tries to open a graphical file chooser (zenity or kdialog).
     *
     * @param extension file extension filter
     * @return selected path, or {@code null} if unavailable
     */
    private String openFileChooser(final String extension) {
        String[][] candidates = {
            {"zenity", "--file-selection",
                "--title=Selecionar arquivo",
                "--file-filter=*." + extension},
            {"kdialog", "--getopenfilename", ".",
                "*." + extension}
        };
        for (String[] cmd : candidates) {
            try {
                Process proc = new ProcessBuilder(cmd)
                    .redirectErrorStream(true)
                    .start();
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(
                            proc.getInputStream()))) {
                    String path = br.readLine();
                    int exit = proc.waitFor();
                    if (exit == 0
                            && path != null
                            && !path.isBlank()) {
                        return path.trim();
                    }
                }
            } catch (IOException | InterruptedException ignored) {
                // try next candidate
            }
        }
        return null;
    }

    /**
     * Recursively finds files matching the given extension,
     * skipping hidden directories and known build/IDE folders.
     *
     * @param dir       directory to search
     * @param extension file extension without dot
     * @return sorted list of matching files
     */
    private List<File> findFiles(
            final File dir, final String extension) {
        List<File> result = new ArrayList<>();
        File[] entries = dir.listFiles();
        if (entries == null) {
            return result;
        }
        Arrays.sort(entries);
        for (File f : entries) {
            if (f.isDirectory()) {
                if (!f.getName().startsWith(".")
                        && !SKIP_DIRS.contains(f.getName())) {
                    result.addAll(findFiles(f, extension));
                }
            } else if (!f.getName().startsWith(".")
                    && f.getName().toLowerCase()
                        .endsWith("." + extension)) {
                result.add(f);
            }
        }
        return result;
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
