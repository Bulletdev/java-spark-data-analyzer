package com.dataanalyzer;

import com.dataanalyzer.core.AggFunction;
import com.dataanalyzer.core.ColumnType;
import com.dataanalyzer.core.DataAggregator;
import com.dataanalyzer.core.DataExporter;
import com.dataanalyzer.core.DataJoiner;
import com.dataanalyzer.core.DataLoader;
import com.dataanalyzer.core.DataProfiler;
import com.dataanalyzer.core.DataTransformer;
import com.dataanalyzer.core.ExportFormat;
import com.dataanalyzer.core.JoinType;
import com.dataanalyzer.core.SqlExecutor;
import com.dataanalyzer.core.WindowFunctionType;
import com.dataanalyzer.ui.InputReader;
import com.dataanalyzer.ui.MenuRenderer;
import com.dataanalyzer.util.OperationHistory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Drives the interactive CLI session.
 *
 * <p>Holds the current {@link Dataset} as mutable state and routes each
 * menu selection to the appropriate service class.
 */
public class UserInterface {

    /** Logger for this class. */
    private static final Logger LOG =
        LoggerFactory.getLogger(UserInterface.class);

    /** Menu option: show sample. */
    private static final int OPT_SHOW_SAMPLE = 3;

    /** Menu option: show stats. */
    private static final int OPT_SHOW_STATS = 4;

    /** Menu option: filter. */
    private static final int OPT_FILTER = 5;

    /** Menu option: aggregate. */
    private static final int OPT_AGGREGATE = 6;

    /** Menu option: transform. */
    private static final int OPT_TRANSFORM = 7;

    /** Menu option: export. */
    private static final int OPT_EXPORT = 8;

    /** Menu option: exit. */
    private static final int OPT_EXIT = 9;

    /** Menu option: SQL. */
    private static final int OPT_SQL = 10;

    /** Menu option: join. */
    private static final int OPT_JOIN = 11;

    /** Menu option: profile. */
    private static final int OPT_PROFILE = 12;

    /** Menu option: history. */
    private static final int OPT_HISTORY = 13;

    /** Transform option: add column. */
    private static final int TOPT_ADD_COL = 3;

    /** Transform option: sort. */
    private static final int TOPT_SORT = 4;

    /** Transform option: remove duplicates. */
    private static final int TOPT_REMOVE_DUPS = 5;

    /** Transform option: remove nulls. */
    private static final int TOPT_REMOVE_NULLS = 6;

    /** Transform option: legacy back. */
    private static final int TOPT_LEGACY_BACK = 7;

    /** Transform option: cast type. */
    private static final int TOPT_CAST = 8;

    /** Transform option: fill null. */
    private static final int TOPT_FILL_NULL = 9;

    /** Transform option: sample. */
    private static final int TOPT_SAMPLE = 10;

    /** Transform option: window function. */
    private static final int TOPT_WINDOW = 11;

    /** Transform option: back. */
    private static final int TOPT_BACK = 12;

    /** Load format option: JSON. */
    private static final int FMT_JSON = 3;

    /** Default rows to show in sample view. */
    private static final int SAMPLE_ROWS = 20;

    /** Rows to show for SQL results. */
    private static final int SQL_ROWS = 50;

    /** Max characters for SQL history entry. */
    private static final int SQL_HIST_LEN = 60;

    /** Data loader service. */
    private final DataLoader loader;

    /** Data transformation service. */
    private final DataTransformer transformer;

    /** Data aggregation service. */
    private final DataAggregator aggregator;

    /** Data export service. */
    private final DataExporter exporter;

    /** User input reader. */
    private final InputReader input;

    /** Menu renderer. */
    private final MenuRenderer menu;

    /** SQL execution service. */
    private final SqlExecutor sqlExecutor;

    /** Join service. */
    private final DataJoiner joiner;

    /** Data profiling service. */
    private final DataProfiler profiler;

    /** Operation history tracker. */
    private final OperationHistory history;

    /** Currently loaded primary DataFrame. */
    private Dataset<Row> currentDf = null;

    /** Currently loaded secondary DataFrame for joins. */
    private Dataset<Row> secondaryDf = null;

    /**
     * Constructs the UI from a grouped services holder.
     *
     * @param services all service dependencies required by the UI
     */
    public UserInterface(final UserInterfaceServices services) {
        this.loader = services.getLoader();
        this.transformer = services.getTransformer();
        this.aggregator = services.getAggregator();
        this.exporter = services.getExporter();
        this.input = services.getInput();
        this.menu = services.getMenu();
        this.sqlExecutor = services.getSqlExecutor();
        this.joiner = services.getJoiner();
        this.profiler = services.getProfiler();
        this.history = services.getHistory();
    }

    /** Starts the interactive loop, blocking until the user exits. */
    public void run() {
        boolean running = true;
        while (running) {
            menu.displayMainMenu();
            int choice = readChoice();

            switch (choice) {
                case 1:
                    handleLoad();
                    break;
                case 2:
                    handleShowSchema();
                    break;
                case OPT_SHOW_SAMPLE:
                    handleShowSample();
                    break;
                case OPT_SHOW_STATS:
                    handleShowStats();
                    break;
                case OPT_FILTER:
                    handleFilter();
                    break;
                case OPT_AGGREGATE:
                    handleAggregate();
                    break;
                case OPT_TRANSFORM:
                    handleTransform();
                    break;
                case OPT_EXPORT:
                    handleExport();
                    break;
                case OPT_EXIT:
                    running = false;
                    System.out.println("Encerrando aplicação...");
                    break;
                case OPT_SQL:
                    handleSql();
                    break;
                case OPT_JOIN:
                    handleJoin();
                    break;
                case OPT_PROFILE:
                    handleProfile();
                    break;
                case OPT_HISTORY:
                    handleHistory();
                    break;
                default:
                    menu.printError(
                        "Opção inválida. Digite um número de 1 a 13.");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load
    // -------------------------------------------------------------------------

    private void handleLoad() {
        menu.displayLoadFormatMenu();
        int formatChoice = readChoice();

        try {
            switch (formatChoice) {
                case 1:
                    loadCsv();
                    break;
                case 2:
                    loadParquet();
                    break;
                case FMT_JSON:
                    loadJson();
                    break;
                default:
                    menu.printError("Formato inválido.");
                    return;
            }
        } catch (IllegalArgumentException e) {
            menu.printError(e.getMessage());
        } catch (Exception e) {
            menu.printError("Erro ao carregar dados: " + e.getMessage());
            LOG.error("Load failed", e);
        }
    }

    private void loadCsv() {
        String path = input.readLine(
            "Caminho do CSV (ou 'example' para dados de exemplo): ");

        if (path.equalsIgnoreCase("example")) {
            currentDf = loader.loadSample();
            history.record("Carregar", "CSV exemplo");
        } else {
            boolean hasHeader = input.readBoolean(
                "O arquivo tem cabeçalho? (s/n): ");
            String delimiter = input.readLine(
                "Delimitador (Enter para ','): ");
            if (delimiter.isEmpty()) {
                delimiter = ",";
            }
            currentDf = loader.load(path, hasHeader, delimiter);
            history.record("Carregar CSV", path);
        }
        System.out.println("DataFrame carregado.");
        currentDf.printSchema();
    }

    private void loadParquet() {
        String path = input.readLine(
            "Caminho do arquivo/diretório Parquet: ");
        currentDf = loader.loadParquet(path);
        history.record("Carregar Parquet", path);
        System.out.println("DataFrame carregado.");
        currentDf.printSchema();
    }

    private void loadJson() {
        String path = input.readLine(
            "Caminho do arquivo/diretório JSON: ");
        currentDf = loader.loadJson(path);
        history.record("Carregar JSON", path);
        System.out.println("DataFrame carregado.");
        currentDf.printSchema();
    }

    // -------------------------------------------------------------------------
    // Exploration
    // -------------------------------------------------------------------------

    private void handleShowSchema() {
        if (!checkLoaded()) {
            return;
        }
        menu.printSection("Schema");
        currentDf.printSchema();
    }

    private void handleShowSample() {
        if (!checkLoaded()) {
            return;
        }
        menu.printSection("Amostra de Dados (20 linhas)");
        currentDf.show(SAMPLE_ROWS, false);
    }

    private void handleShowStats() {
        if (!checkLoaded()) {
            return;
        }
        menu.printSection("Estatísticas Descritivas");
        System.out.println("Calculando...");
        currentDf.describe().show();

        System.out.println("Valores nulos por coluna:");
        for (String colName : currentDf.columns()) {
            long nullCount =
                currentDf.filter(col(colName).isNull()).count();
            System.out.printf("  %-25s %d%n", colName + ":", nullCount);
        }
    }

    // -------------------------------------------------------------------------
    // Filter
    // -------------------------------------------------------------------------

    private void handleFilter() {
        if (!checkLoaded()) {
            return;
        }
        menu.printColumns(currentDf.columns());

        String column   = input.readLine("Coluna para filtrar: ");
        String operator = input.readLine(
            "Operador (=, >, <, >=, <=, !=): ");
        String value    = input.readLine("Valor: ");

        try {
            currentDf = transformer.filter(
                currentDf, column, operator, value);
            history.record(
                "Filtro", column + " " + operator + " " + value);
        } catch (IllegalArgumentException e) {
            menu.printError(e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Aggregate
    // -------------------------------------------------------------------------

    private void handleAggregate() {
        if (!checkLoaded()) {
            return;
        }
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
            result.show(SAMPLE_ROWS, false);

            if (input.readBoolean(
                    "Usar como novo DataFrame? (s/n): ")) {
                currentDf = result;
                history.record(
                    "Agregação", fn.name() + " em " + aggCol);
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
        if (!checkLoaded()) {
            return;
        }
        menu.displayTransformMenu();
        int choice = readChoice();

        try {
            switch (choice) {
                case 1:
                    currentDf = doSelectColumns();
                    history.record("Selecionar colunas", "");
                    break;
                case 2:
                    currentDf = doRenameColumn();
                    history.record("Renomear coluna", "");
                    break;
                case TOPT_ADD_COL:
                    currentDf = doAddColumn();
                    history.record("Adicionar coluna", "");
                    break;
                case TOPT_SORT:
                    currentDf = doSort();
                    history.record("Ordenar", "");
                    break;
                case TOPT_REMOVE_DUPS:
                    currentDf = doRemoveDuplicates();
                    history.record("Remover duplicatas", "");
                    break;
                case TOPT_REMOVE_NULLS:
                    currentDf = doRemoveNulls();
                    history.record("Remover nulos", "");
                    break;
                case TOPT_LEGACY_BACK:
                    return;
                case TOPT_CAST:
                    currentDf = doCast();
                    history.record("Cast tipo", "");
                    break;
                case TOPT_FILL_NULL:
                    currentDf = doFillNull();
                    history.record("Preencher nulos", "");
                    break;
                case TOPT_SAMPLE:
                    currentDf = doSample();
                    history.record("Amostragem", "");
                    break;
                case TOPT_WINDOW:
                    currentDf = doWindowFunction();
                    history.record("Função de janela", "");
                    break;
                case TOPT_BACK:
                    return;
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
        System.out.println(
            "  case when Preco > 100 then 'Caro' else 'Barato' end");
        String name = input.readLine("Nome da nova coluna: ");
        String expr = input.readLine("Expressão SQL: ");
        return transformer.addColumn(currentDf, name, expr);
    }

    private Dataset<Row> doSort() {
        menu.printColumns(currentDf.columns());
        List<String> cols = input.readList(
            "Colunas para ordenar (separadas por vírgula): ");
        boolean asc = input.readBoolean("Ordem ascendente? (s/n): ");
        return transformer.sort(
            currentDf, cols.toArray(new String[0]), asc);
    }

    private Dataset<Row> doRemoveDuplicates() {
        boolean exact = input.readBoolean(
            "Remover duplicatas exatas (todas as colunas)? (s/n): ");
        if (exact) {
            return transformer.removeDuplicates(
                currentDf, new String[0]);
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
            return transformer.removeNulls(
                currentDf, "any", new String[0]);
        }
        boolean all = input.readBoolean(
            "Remover somente quando *todas* as colunas são nulas?"
            + " (s/n): ");
        String mode = all ? "all" : "any";
        menu.printColumns(currentDf.columns());
        List<String> cols = input.readList(
            "Colunas a verificar (Enter para todas): ");
        return transformer.removeNulls(
            currentDf, mode, cols.toArray(new String[0]));
    }

    private Dataset<Row> doCast() {
        menu.printColumns(currentDf.columns());
        String column = input.readLine("Coluna para converter: ");
        System.out.println("Tipos disponíveis: "
            + Arrays.toString(ColumnType.values()));
        String targetType = input.readLine(
            "Tipo de destino (ex: int, double, string, date): ");
        return transformer.castColumn(currentDf, column, targetType);
    }

    private Dataset<Row> doFillNull() {
        menu.printColumns(currentDf.columns());
        String column = input.readLine(
            "Coluna para preencher nulos: ");
        String value  = input.readLine("Valor de preenchimento: ");
        return transformer.fillNull(currentDf, column, value);
    }

    private Dataset<Row> doSample() {
        String fractionStr = input.readLine(
            "Fração da amostra (0.0–1.0, ex: 0.1 para 10%): ");
        double fraction;
        try {
            fraction = Double.parseDouble(fractionStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Fração inválida: " + fractionStr);
        }
        boolean withSeed = input.readBoolean(
            "Usar semente fixa para reprodutibilidade? (s/n): ");
        return transformer.sample(currentDf, fraction, withSeed);
    }

    private Dataset<Row> doWindowFunction() {
        menu.printColumns(currentDf.columns());
        String newColName = input.readLine("Nome da nova coluna: ");
        menu.displayWindowFunctionMenu();
        WindowFunctionType fn = toWindowFunction(readChoice());
        if (fn == null) {
            throw new IllegalArgumentException(
                "Função de janela inválida.");
        }
        String partitionBy = input.readLine(
            "Coluna de partição (Enter para sem partição): ");
        if (partitionBy.isEmpty()) {
            partitionBy = null;
        }
        String orderBy = input.readLine("Coluna de ordenação: ");
        int offset = 0;
        if (fn == WindowFunctionType.LAG
                || fn == WindowFunctionType.LEAD) {
            String offsetStr = input.readLine(
                "Offset (número de linhas): ");
            try {
                offset = Integer.parseInt(offsetStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Offset inválido: " + offsetStr);
            }
        }
        return transformer.applyWindowFunction(
            currentDf, newColName, fn, partitionBy, orderBy, offset);
    }

    // -------------------------------------------------------------------------
    // SQL
    // -------------------------------------------------------------------------

    private void handleSql() {
        if (!checkLoaded()) {
            return;
        }
        System.out.println(
            "DataFrame disponível como: "
            + sqlExecutor.getViewName());
        System.out.println(
            "Exemplo: SELECT Categoria, COUNT(*) FROM df"
            + " GROUP BY Categoria");
        String query = input.readLine("SQL> ");
        try {
            Dataset<Row> result = sqlExecutor.execute(currentDf, query);
            result.show(SQL_ROWS, false);
            if (input.readBoolean(
                    "Adotar como DataFrame atual? (s/n): ")) {
                currentDf = result;
                history.record("SQL", query.length() > SQL_HIST_LEN
                    ? query.substring(0, SQL_HIST_LEN) + "..." : query);
                menu.printSuccess("DataFrame atualizado.");
            }
        } catch (Exception e) {
            menu.printError(e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Join
    // -------------------------------------------------------------------------

    private void handleJoin() {
        if (!checkLoaded()) {
            return;
        }

        System.out.println("Carregando segundo arquivo para join...");
        menu.displayLoadFormatMenu();
        int formatChoice = readChoice();

        try {
            switch (formatChoice) {
                case 1:
                    secondaryDf = loadSecondaryFile();
                    break;
                case 2:
                    String parquetPath = input.readLine(
                        "Caminho do arquivo/diretório Parquet: ");
                    secondaryDf = loader.loadParquet(parquetPath);
                    break;
                case FMT_JSON:
                    String jsonPath = input.readLine(
                        "Caminho do arquivo/diretório JSON: ");
                    secondaryDf = loader.loadJson(jsonPath);
                    break;
                default:
                    menu.printError("Formato inválido.");
                    return;
            }
        } catch (Exception e) {
            menu.printError(
                "Erro ao carregar segundo arquivo: "
                + e.getMessage());
            return;
        }

        menu.printSection("Schema — DataFrame Principal (A)");
        currentDf.printSchema();
        menu.printSection("Schema — DataFrame Secundário (B)");
        secondaryDf.printSchema();

        List<String> keys = input.readList(
            "Coluna(s) chave para join (separadas por vírgula): ");
        if (keys.isEmpty()) {
            menu.printError(
                "É necessário informar pelo menos uma coluna chave.");
            return;
        }

        menu.displayJoinTypeMenu();
        JoinType joinType = toJoinType(readChoice());
        if (joinType == null) {
            menu.printError("Tipo de join inválido.");
            return;
        }

        try {
            Dataset<Row> result = keys.size() == 1
                ? joiner.join(
                    currentDf, secondaryDf, keys.get(0), joinType)
                : joiner.join(
                    currentDf, secondaryDf, keys, joinType);

            menu.printSection("Schema do Resultado");
            result.printSchema();
            result.show(SAMPLE_ROWS, false);

            if (input.readBoolean(
                    "Adotar como DataFrame atual? (s/n): ")) {
                currentDf = result;
                history.record(
                    "Join",
                    joinType.getSparkName() + " por " + keys);
                menu.printSuccess("DataFrame atualizado.");
            }
        } catch (IllegalArgumentException e) {
            menu.printError(e.getMessage());
        }
    }

    private Dataset<Row> loadSecondaryFile() {
        String path = input.readLine(
            "Caminho do CSV (ou 'example' para dados de exemplo): ");
        if (path.equalsIgnoreCase("example")) {
            return loader.loadSample();
        }
        boolean hasHeader = input.readBoolean(
            "O arquivo tem cabeçalho? (s/n): ");
        String delimiter = input.readLine(
            "Delimitador (Enter para ','): ");
        if (delimiter.isEmpty()) {
            delimiter = ",";
        }
        return loader.load(path, hasHeader, delimiter);
    }

    // -------------------------------------------------------------------------
    // Profile
    // -------------------------------------------------------------------------

    private void handleProfile() {
        if (!checkLoaded()) {
            return;
        }
        profiler.profile(currentDf);
        history.record(
            "Perfil de dados",
            currentDf.columns().length + " colunas");
    }

    // -------------------------------------------------------------------------
    // History
    // -------------------------------------------------------------------------

    private void handleHistory() {
        history.print();
    }

    // -------------------------------------------------------------------------
    // Export
    // -------------------------------------------------------------------------

    private void handleExport() {
        if (!checkLoaded()) {
            return;
        }
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

        boolean overwrite = input.readBoolean(
            "Sobrescrever se existir? (s/n): ");
        boolean singleFile = input.readBoolean(
            "Exportar como arquivo único?"
            + " Pode ser lento para dados grandes. (s/n): ");

        try {
            if (format == ExportFormat.CSV) {
                boolean header = input.readBoolean(
                    "Incluir cabeçalho? (s/n): ");
                String delim = input.readLine(
                    "Delimitador (Enter para ','): ");
                if (delim.isEmpty()) {
                    delim = ",";
                }
                exporter.export(currentDf, path, format, overwrite,
                    singleFile, header, delim);
            } else {
                exporter.export(currentDf, path, format, overwrite,
                    singleFile, true, ",");
            }
            history.record(
                "Exportar", format.name() + " → " + path);
        } catch (Exception e) {
            menu.printError("Erro ao salvar: " + e.getMessage());
            LOG.error("Export failed", e);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private boolean checkLoaded() {
        if (currentDf == null) {
            menu.printError(
                "Nenhum dado carregado. Use a opção 1 primeiro.");
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

    private AggFunction toAggFunction(final int choice) {
        switch (choice) {
            case 1: return AggFunction.AVG;
            case 2: return AggFunction.SUM;
            case FMT_JSON: return AggFunction.MIN;
            case TOPT_SORT: return AggFunction.MAX;
            case OPT_FILTER: return AggFunction.COUNT;
            default: return null;
        }
    }

    private ExportFormat toExportFormat(final int choice) {
        switch (choice) {
            case 1: return ExportFormat.CSV;
            case 2: return ExportFormat.PARQUET;
            case FMT_JSON: return ExportFormat.JSON;
            default: return null;
        }
    }

    private JoinType toJoinType(final int choice) {
        switch (choice) {
            case 1: return JoinType.INNER;
            case 2: return JoinType.LEFT;
            case FMT_JSON: return JoinType.RIGHT;
            case TOPT_SORT: return JoinType.FULL;
            default: return null;
        }
    }

    private WindowFunctionType toWindowFunction(final int choice) {
        switch (choice) {
            case 1: return WindowFunctionType.RANK;
            case 2: return WindowFunctionType.DENSE_RANK;
            case FMT_JSON: return WindowFunctionType.ROW_NUMBER;
            case TOPT_SORT: return WindowFunctionType.LAG;
            case OPT_FILTER: return WindowFunctionType.LEAD;
            default: return null;
        }
    }
}
