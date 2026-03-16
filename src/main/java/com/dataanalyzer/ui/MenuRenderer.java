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
        System.out.println("1.  Carregar dados");
        System.out.println("2.  Visualizar schema");
        System.out.println("3.  Mostrar amostra de dados");
        System.out.println("4.  Estatísticas descritivas");
        System.out.println("5.  Filtrar dados");
        System.out.println("6.  Agregar dados");
        System.out.println("7.  Transformar dados");
        System.out.println("8.  Salvar resultados");
        System.out.println("9.  Sair");
        System.out.println("10. SQL interativo");
        System.out.println("11. Join entre arquivos");
        System.out.println("12. Perfil de dados");
        System.out.println("13. Histórico de operações");
        System.out.println("---------------------------------");
        System.out.print("Escolha (1-13): ");
    }

    /** Renders the transformation sub-menu. */
    public void displayTransformMenu() {
        System.out.println("\nTransformações disponíveis:");
        System.out.println("1.  Selecionar colunas");
        System.out.println("2.  Renomear coluna");
        System.out.println("3.  Criar nova coluna (expressão SQL)");
        System.out.println("4.  Ordenar dados");
        System.out.println("5.  Remover duplicatas");
        System.out.println("6.  Remover valores nulos");
        System.out.println("7.  Nada (voltar) — [opção legado]");
        System.out.println("8.  Cast de tipo (converter coluna)");
        System.out.println("9.  Preencher nulos (fillna)");
        System.out.println("10. Amostrar dados (sample)");
        System.out.println("11. Funções de janela");
        System.out.println("12. Voltar");
        System.out.print("Transformação (1-12): ");
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
     * Renders the file format selection menu used during load operations.
     */
    public void displayLoadFormatMenu() {
        System.out.println("\nFormato do arquivo:");
        System.out.println("1. CSV");
        System.out.println("2. Parquet");
        System.out.println("3. JSON");
        System.out.print("Formato (1-3): ");
    }

    /**
     * Renders the join type selection menu.
     */
    public void displayJoinTypeMenu() {
        System.out.println("\nTipo de join:");
        System.out.println("1. INNER");
        System.out.println("2. LEFT");
        System.out.println("3. RIGHT");
        System.out.println("4. FULL OUTER");
        System.out.print("Tipo (1-4): ");
    }

    /**
     * Renders the window function type selection menu.
     */
    public void displayWindowFunctionMenu() {
        System.out.println("\nFunção de janela:");
        System.out.println("1. RANK");
        System.out.println("2. DENSE_RANK");
        System.out.println("3. ROW_NUMBER");
        System.out.println("4. LAG");
        System.out.println("5. LEAD");
        System.out.print("Função (1-5): ");
    }

    /**
     * Prints a labelled section separator.
     *
     * @param label section title
     */
    public void printSection(final String label) {
        System.out.println("\n--- " + label + " ---");
    }

    /**
     * Prints an error message prefixed with {@code [ERRO]}.
     *
     * @param message error description
     */
    public void printError(final String message) {
        System.out.println("[ERRO] " + message);
    }

    /**
     * Prints a success message prefixed with {@code [OK]}.
     *
     * @param message success description
     */
    public void printSuccess(final String message) {
        System.out.println("[OK] " + message);
    }

    /**
     * Prints the available column names joined by commas.
     *
     * @param columns array of column names
     */
    public void printColumns(final String[] columns) {
        System.out.println("Colunas disponíveis: "
            + String.join(", ", columns));
    }
}
