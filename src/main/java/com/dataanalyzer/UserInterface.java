package com.dataanalyzer;

import com.dataanalyzer.core.AggFunction;
import com.dataanalyzer.core.DataAggregator;
import com.dataanalyzer.core.DataExporter;
import com.dataanalyzer.core.DataLoader;
import com.dataanalyzer.core.DataTransformer;
import com.dataanalyzer.core.ExportFormat;
import com.dataanalyzer.ui.InputReader;
import com.dataanalyzer.ui.MenuRenderer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Drives the interactive CLI session.
 *
 * <p>Holds the current {@link Dataset} as mutable state and routes each
 * menu selection to the appropriate service class.
 */
public class UserInterface {

    private static final Logger log =
        LoggerFactory.getLogger(UserInterface.class);

    private final DataLoader loader;
    private final DataTransformer transformer;
    private final DataAggregator aggregator;
    private final DataExporter exporter;
    private final InputReader input;
    private final MenuRenderer menu;

    private Dataset<Row> currentDf = null;

    /**
     * @param loader      data loading service
     * @param transformer transformation service
     * @param aggregator  aggregation service
     * @param exporter    export service
     * @param input       user input reader
     * @param menu        menu renderer
     */
    public UserInterface(
            DataLoader loader,
            DataTransformer transformer,
            DataAggregator aggregator,
            DataExporter exporter,
            InputReader input,
            MenuRenderer menu) {
        this.loader = loader;
        this.transformer = transformer;
        this.aggregator = aggregator;
        this.exporter = exporter;
        this.input = input;
        this.menu = menu;
    }

    /** Starts the interactive loop, blocking until the user exits. */
    public void run() {
        boolean running = true;
        while (running) {
            menu.displayMainMenu();
            int choice = readChoice();

            switch (choice) {
                case 1: handleLoad();        break;
                case 2: handleShowSchema();  break;
                case 3: handleShowSample();  break;
                case 4: handleShowStats();   break;
                case 5: handleFilter();      break;
                case 6: handleAggregate();   break;
                case 7: handleTransform();   break;
                case 8: handleExport();      break;
                case 9:
                    running = false;
                    System.out.println("Encerrando aplicação...");
                    break;
                default:
                    menu.printError("Opção inválida. Digite um número de 1 a 9.");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load
    // -------------------------------------------------------------------------

    private void handleLoad() {
        String path = input.readLine(
            "Caminho do CSV (ou 'example' para dados de exemplo): ");

        try {
            if (path.equalsIgnoreCase("example")) {
                currentDf = loader.loadSample();
            } else {
                boolean hasHeader = input.readBoolean(
                    "O arquivo tem cabeçalho? (s/n): ");
                String delimiter = input.readLine(
                    "Delimitador (Enter para ','): ");
                if (delimiter.isEmpty()) delimiter = ",";
                currentDf = loader.load(path, hasHeader, delimiter);
            }
        } catch (IllegalArgumentException e) {
            menu.printError(e.getMessage());
        } catch (Exception e) {
            menu.printError("Erro ao carregar dados: " + e.getMessage());
            log.error("Load failed", e);
        }
    }

    // -------------------------------------------------------------------------
    // Exploration
    // -------------------------------------------------------------------------

    private void handleShowSchema() {
        if (!checkLoaded()) return;
        menu.printSection("Schema");
        currentDf.printSchema();
    }

    private void handleShowSample() {
        if (!checkLoaded()) return;
        menu.printSection("Amostra de Dados (20 linhas)");
        currentDf.show(20, false);
    }

    private void handleShowStats() {
        if (!checkLoaded()) return;
        menu.printSection("Estatísticas Descritivas");
        System.out.println("Calculando...");
        currentDf.describe().show();

        System.out.println("Valores nulos por coluna:");
        for (String colName : currentDf.columns()) {
            long nullCount = currentDf.filter(col(colName).isNull()).count();
            System.out.printf("  %-25s %d%n", colName + ":", nullCount);
        }
    }

    // -------------------------------------------------------------------------
    // Filter
    // -------------------------------------------------------------------------

    private void handleFilter() {
        if (!checkLoaded()) return;
        menu.printColumns(currentDf.columns());

        String column   = input.readLine("Coluna para filtrar: ");
        String operator = input.readLine("Operador (=, >, <, >=, <=, !=): ");
        String value    = input.readLine("Valor: ");

        try {
            currentDf = transformer.filter(currentDf, column, operator, value);
        } catch (IllegalArgumentException e) {
            menu.printError(e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Aggregate
    // -------------------------------------------------------------------------

    private void handleAggregate() {
        if (!checkLoaded()) return;
        menu.printColumns(currentDf.columns());

        String groupBy = input.readLine(
            "Coluna para agrupar (Enter para não agrupar): ");
        String aggCol = input.readLine("Coluna para agregar: ");

        menu.displayAggMenu();
        AggFunction fn = toAggFunction(readChoice());
        if (fn == null) {
            menu.printError("Função inválida.");
            return;
        }

        try {
            Dataset<Row> result = aggregator.aggregate(
                currentDf, aggCol, fn,
                groupBy.isEmpty() ? null : groupBy);

            menu.printSection("Resultado da Agregação");
            result.show(20, false);

            if (input.readBoolean("Usar como novo DataFrame? (s/n): ")) {
                currentDf = result;
                menu.printSuccess("DataFrame atualizado.");
                currentDf.printSchema();
            }
        } catch (IllegalArgumentException e) {
            menu.printError(e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Transform
    // -------------------------------------------------------------------------

    private void handleTransform() {
        if (!checkLoaded()) return;
        menu.displayTransformMenu();
        int choice = readChoice();

        try {
            switch (choice) {
                case 1: currentDf = doSelectColumns();    break;
                case 2: currentDf = doRenameColumn();     break;
                case 3: currentDf = doAddColumn();        break;
                case 4: currentDf = doSort();             break;
                case 5: currentDf = doRemoveDuplicates(); break;
                case 6: currentDf = doRemoveNulls();      break;
                case 7: return;
                default:
                    menu.printError("Opção inválida.");
                    return;
            }
            menu.printSection("Schema Atualizado");
            currentDf.printSchema();
        } catch (IllegalArgumentException e) {
            menu.printError(e.getMessage());
        }
    }

    private Dataset<Row> doSelectColumns() {
        menu.printColumns(currentDf.columns());
        List<String> cols = input.readList(
            "Colunas a selecionar (separadas por vírgula): ");
        return transformer.selectColumns(currentDf, cols);
    }

    private Dataset<Row> doRenameColumn() {
        menu.printColumns(currentDf.columns());
        String oldName = input.readLine("Coluna a renomear: ");
        String newName = input.readLine("Novo nome: ");
        return transformer.renameColumn(currentDf, oldName, newName);
    }

    private Dataset<Row> doAddColumn() {
        menu.printColumns(currentDf.columns());
        System.out.println("Exemplos de expressão:");
        System.out.println("  Preco * Quantidade");
        System.out.println("  upper(Produto)");
        System.out.println("  case when Preco > 100 then 'Caro' else 'Barato' end");
        String name = input.readLine("Nome da nova coluna: ");
        String expr = input.readLine("Expressão SQL: ");
        return transformer.addColumn(currentDf, name, expr);
    }

    private Dataset<Row> doSort() {
        menu.printColumns(currentDf.columns());
        List<String> cols = input.readList(
            "Colunas para ordenar (separadas por vírgula): ");
        boolean asc = input.readBoolean("Ordem ascendente? (s/n): ");
        return transformer.sort(currentDf, cols.toArray(new String[0]), asc);
    }

    private Dataset<Row> doRemoveDuplicates() {
        boolean exact = input.readBoolean(
            "Remover duplicatas exatas (todas as colunas)? (s/n): ");
        if (exact) {
            return transformer.removeDuplicates(currentDf, new String[0]);
        }
        menu.printColumns(currentDf.columns());
        List<String> keys = input.readList(
            "Colunas chave para identificar duplicatas: ");
        return transformer.removeDuplicates(
            currentDf, keys.toArray(new String[0]));
    }

    private Dataset<Row> doRemoveNulls() {
        boolean dropAny = input.readBoolean(
            "Remover linhas com *qualquer* valor nulo? (s/n): ");
        if (dropAny) {
            return transformer.removeNulls(currentDf, "any", new String[0]);
        }
        boolean all = input.readBoolean(
            "Remover somente quando *todas* as colunas são nulas? (s/n): ");
        String mode = all ? "all" : "any";
        menu.printColumns(currentDf.columns());
        List<String> cols = input.readList(
            "Colunas a verificar (Enter para todas): ");
        return transformer.removeNulls(
            currentDf, mode, cols.toArray(new String[0]));
    }

    // -------------------------------------------------------------------------
    // Export
    // -------------------------------------------------------------------------

    private void handleExport() {
        if (!checkLoaded()) return;
        menu.displayExportMenu();

        ExportFormat format = toExportFormat(readChoice());
        if (format == null) {
            menu.printError("Formato inválido.");
            return;
        }

        String path = input.readLine("Diretório de destino: ");
        if (path.isEmpty()) {
            menu.printError("Caminho não pode ser vazio.");
            return;
        }

        boolean overwrite = input.readBoolean("Sobrescrever se existir? (s/n): ");

        try {
            if (format == ExportFormat.CSV) {
                boolean header = input.readBoolean("Incluir cabeçalho? (s/n): ");
                String delim = input.readLine("Delimitador (Enter para ','): ");
                if (delim.isEmpty()) delim = ",";
                exporter.export(currentDf, path, format, overwrite, header, delim);
            } else {
                exporter.export(currentDf, path, format, overwrite);
            }
        } catch (Exception e) {
            menu.printError("Erro ao salvar: " + e.getMessage());
            log.error("Export failed", e);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private boolean checkLoaded() {
        if (currentDf == null) {
            menu.printError("Nenhum dado carregado. Use a opção 1 primeiro.");
            return false;
        }
        return true;
    }

    private int readChoice() {
        try {
            return Integer.parseInt(input.readLine(""));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private AggFunction toAggFunction(int choice) {
        switch (choice) {
            case 1: return AggFunction.AVG;
            case 2: return AggFunction.SUM;
            case 3: return AggFunction.MIN;
            case 4: return AggFunction.MAX;
            case 5: return AggFunction.COUNT;
            default: return null;
        }
    }

    private ExportFormat toExportFormat(int choice) {
        switch (choice) {
            case 1: return ExportFormat.CSV;
            case 2: return ExportFormat.PARQUET;
            case 3: return ExportFormat.JSON;
            default: return null;
        }
    }
}
