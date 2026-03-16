package com.dataanalyzer.ui;

/**
 * Renders all CLI menus and formatted messages to stdout.
 *
 * <p>Centralising presentation here keeps business logic classes free of
 * display concerns and makes it easy to change the UI without touching
 * the application flow.
 */
public class MenuRenderer {

    /** Renders the main application menu. */
    public void displayMainMenu() {
        System.out.println("\n=================================");
        System.out.println("       Java Data Analyzer        ");
        System.out.println("=================================");
        System.out.println("1. Carregar dados");
        System.out.println("2. Visualizar schema");
        System.out.println("3. Mostrar amostra de dados");
        System.out.println("4. Estatísticas descritivas");
        System.out.println("5. Filtrar dados");
        System.out.println("6. Agregar dados");
        System.out.println("7. Transformar dados");
        System.out.println("8. Salvar resultados");
        System.out.println("9. Sair");
        System.out.println("---------------------------------");
        System.out.print("Escolha (1-9): ");
    }

    /** Renders the transformation sub-menu. */
    public void displayTransformMenu() {
        System.out.println("\nTransformações disponíveis:");
        System.out.println("1. Selecionar colunas");
        System.out.println("2. Renomear coluna");
        System.out.println("3. Criar nova coluna (expressão SQL)");
        System.out.println("4. Ordenar dados");
        System.out.println("5. Remover duplicatas");
        System.out.println("6. Remover valores nulos");
        System.out.println("7. Voltar");
        System.out.print("Transformação (1-7): ");
    }

    /** Renders the aggregation function sub-menu. */
    public void displayAggMenu() {
        System.out.println("\nFunções de agregação:");
        System.out.println("1. Média (avg)");
        System.out.println("2. Soma (sum)");
        System.out.println("3. Mínimo (min)");
        System.out.println("4. Máximo (max)");
        System.out.println("5. Contagem (count)");
        System.out.print("Função (1-5): ");
    }

    /** Renders the export format sub-menu. */
    public void displayExportMenu() {
        System.out.println("\nFormatos de exportação:");
        System.out.println("1. CSV");
        System.out.println("2. Parquet");
        System.out.println("3. JSON");
        System.out.print("Formato (1-3): ");
    }

    /**
     * Prints a labelled section separator.
     *
     * @param label section title
     */
    public void printSection(String label) {
        System.out.println("\n--- " + label + " ---");
    }

    /**
     * Prints an error message prefixed with {@code [ERRO]}.
     *
     * @param message error description
     */
    public void printError(String message) {
        System.out.println("[ERRO] " + message);
    }

    /**
     * Prints a success message prefixed with {@code [OK]}.
     *
     * @param message success description
     */
    public void printSuccess(String message) {
        System.out.println("[OK] " + message);
    }

    /**
     * Prints the available column names joined by commas.
     *
     * @param columns array of column names
     */
    public void printColumns(String[] columns) {
        System.out.println("Colunas disponíveis: "
            + String.join(", ", columns));
    }
}
