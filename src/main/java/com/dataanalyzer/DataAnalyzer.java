package com.dataanalyzer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import java.io.File;
import java.util.Arrays;
import java.util.Scanner;

public class DataAnalyzer {

    private SparkSession spark;
    private Dataset<Row> dataFrame;

    public static void main(String[] args) {
        DataAnalyzer analyzer = new DataAnalyzer();
        analyzer.initialize();
        analyzer.runInteractive();
    }


    public void initialize() {
        spark = SparkSession.builder()
                .appName("Java Data Analyzer")
                .master("local[*]") // Usa todos os cores disponíveis localmente
                .config("spark.sql.warehouse.dir", "spark-warehouse")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("Spark inicializado com sucesso!");
        System.out.println("Versão: " + spark.version());
    }


    public void runInteractive() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        System.out.println("\n=== Java Data Analyzer ===");

        while (running) {
            System.out.println("\nOpções disponíveis:");
            System.out.println("1. Carregar dados de CSV");
            System.out.println("2. Visualizar schema");
            System.out.println("3. Mostrar amostra de dados");
            System.out.println("4. Estatísticas descritivas");
            System.out.println("5. Filtrar dados");
            System.out.println("6. Agregar dados");
            System.out.println("7. Realizar transformações");
            System.out.println("8. Salvar resultados");
            System.out.println("9. Sair");

            System.out.print("\nDigite sua escolha (1-9): ");
            int choice = scanner.nextInt();
            scanner.nextLine(); // Limpa o buffer

            switch (choice) {
                case 1:
                    loadData(scanner);
                    break;
                case 2:
                    showSchema();
                    break;
                case 3:
                    showSample();
                    break;
                case 4:
                    showStats();
                    break;
                case 5:
                    filterData(scanner);
                    break;
                case 6:
                    aggregateData(scanner);
                    break;
                case 7:
                    transformData(scanner);
                    break;
                case 8:
                    saveResults(scanner);
                    break;
                case 9:
                    running = false;
                    shutdown();
                    break;
                default:
                    System.out.println("Opção inválida. Tente novamente.");
            }
        }
        scanner.close();
    }


    private void loadData(Scanner scanner) {
        System.out.print("Digite o caminho para o arquivo CSV: ");
        String path = scanner.nextLine();

        File file = new File(path);
        if (!file.exists()) {
            System.out.println("Arquivo não encontrado: " + path);
            return;
        }

        System.out.print("O arquivo tem cabeçalho? (s/n): ");
        boolean hasHeader = scanner.nextLine().toLowerCase().startsWith("s");

        System.out.print("Delimitador (padrão ','): ");
        String delimiter = scanner.nextLine();
        if (delimiter.isEmpty()) {
            delimiter = ",";
        }

        try {
            // Carrega o CSV com as opções especificadas
            dataFrame = spark.read()
                    .option("header", hasHeader)
                    .option("delimiter", delimiter)
                    .option("inferSchema", "true")
                    .csv(path);

            System.out.println("Dados carregados com sucesso!");
            System.out.println("Número de linhas: " + dataFrame.count());
            System.out.println("Número de colunas: " + dataFrame.columns().length);
        } catch (Exception e) {
            System.out.println("Erro ao carregar os dados: " + e.getMessage());
        }
    }


    private void showSchema() {
        if (checkDataFrameLoaded()) {
            System.out.println("Schema do DataFrame:");
            dataFrame.printSchema();
        }
    }


    private void showSample() {
        if (checkDataFrameLoaded()) {
            System.out.println("Amostra de dados (20 primeiras linhas):");
            dataFrame.show(20, false);
        }
    }


    private void showStats() {
        if (checkDataFrameLoaded()) {
            System.out.println("Estatísticas descritivas:");
            dataFrame.describe().show();

            // Mostra contagem de valores nulos por coluna
            System.out.println("Contagem de valores nulos por coluna:");
            for (String col : dataFrame.columns()) {
                long nullCount = dataFrame.filter(col(col).isNull()).count();
                System.out.println(col + ": " + nullCount);
            }
        }
    }


    private void filterData(Scanner scanner) {
        if (!checkDataFrameLoaded()) return;

        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite o nome da coluna para filtrar: ");
        String column = scanner.nextLine();

        if (!Arrays.asList(dataFrame.columns()).contains(column)) {
            System.out.println("Coluna não encontrada!");
            return;
        }

        System.out.print("Digite o operador (=, >, <, >=, <=, !=): ");
        String operator = scanner.nextLine();

        System.out.print("Digite o valor: ");
        String value = scanner.nextLine();

        try {
            switch (operator) {
                case "=":
                    dataFrame = dataFrame.filter(col(column).equalTo(value));
                    break;
                case ">":
                    dataFrame = dataFrame.filter(col(column).gt(value));
                    break;
                case "<":
                    dataFrame = dataFrame.filter(col(column).lt(value));
                    break;
                case ">=":
                    dataFrame = dataFrame.filter(col(column).geq(value));
                    break;
                case "<=":
                    dataFrame = dataFrame.filter(col(column).leq(value));
                    break;
                case "!=":
                    dataFrame = dataFrame.filter(col(column).notEqual(value));
                    break;
                default:
                    System.out.println("Operador inválido!");
                    return;
            }

            System.out.println("Filtro aplicado! Número de linhas após filtro: " + dataFrame.count());
        } catch (Exception e) {
            System.out.println("Erro ao aplicar filtro: " + e.getMessage());
        }
    }


    private void aggregateData(Scanner scanner) {
        if (!checkDataFrameLoaded()) return;

        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite a coluna para agrupar (deixe em branco para não agrupar): ");
        String groupByColumn = scanner.nextLine();

        System.out.print("Digite a coluna para agregar: ");
        String aggregateColumn = scanner.nextLine();

        if (!Arrays.asList(dataFrame.columns()).contains(aggregateColumn)) {
            System.out.println("Coluna para agregação não encontrada!");
            return;
        }

        if (!groupByColumn.isEmpty() && !Arrays.asList(dataFrame.columns()).contains(groupByColumn)) {
            System.out.println("Coluna para agrupamento não encontrada!");
            return;
        }

        System.out.println("Funções de agregação disponíveis:");
        System.out.println("1. Média (avg)");
        System.out.println("2. Soma (sum)");
        System.out.println("3. Mínimo (min)");
        System.out.println("4. Máximo (max)");
        System.out.println("5. Contagem (count)");

        System.out.print("Digite o número da função: ");
        int functionChoice = scanner.nextInt();
        scanner.nextLine(); // Limpa o buffer

        try {
            Dataset<Row> resultDF;

            if (groupByColumn.isEmpty()) {
                switch (functionChoice) {
                    case 1:
                        resultDF = dataFrame.agg(avg(aggregateColumn).alias("avg_" + aggregateColumn));
                        break;
                    case 2:
                        resultDF = dataFrame.agg(sum(aggregateColumn).alias("sum_" + aggregateColumn));
                        break;
                    case 3:
                        resultDF = dataFrame.agg(min(aggregateColumn).alias("min_" + aggregateColumn));
                        break;
                    case 4:
                        resultDF = dataFrame.agg(max(aggregateColumn).alias("max_" + aggregateColumn));
                        break;
                    case 5:
                        resultDF = dataFrame.agg(count(aggregateColumn).alias("count_" + aggregateColumn));
                        break;
                    default:
                        System.out.println("Função inválida!");
                        return;
                }
            } else {
                switch (functionChoice) {
                    case 1:
                        resultDF = dataFrame.groupBy(groupByColumn)
                                .agg(avg(aggregateColumn).alias("avg_" + aggregateColumn))
                                .orderBy(groupByColumn);
                        break;
                    case 2:
                        resultDF = dataFrame.groupBy(groupByColumn)
                                .agg(sum(aggregateColumn).alias("sum_" + aggregateColumn))
                                .orderBy(groupByColumn);
                        break;
                    case 3:
                        resultDF = dataFrame.groupBy(groupByColumn)
                                .agg(min(aggregateColumn).alias("min_" + aggregateColumn))
                                .orderBy(groupByColumn);
                        break;
                    case 4:
                        resultDF = dataFrame.groupBy(groupByColumn)
                                .agg(max(aggregateColumn).alias("max_" + aggregateColumn))
                                .orderBy(groupByColumn);
                        break;
                    case 5:
                        resultDF = dataFrame.groupBy(groupByColumn)
                                .agg(count(aggregateColumn).alias("count_" + aggregateColumn))
                                .orderBy(groupByColumn);
                        break;
                    default:
                        System.out.println("Função inválida!");
                        return;
                }
            }

            System.out.println("Resultado da agregação:");
            resultDF.show(20, false);

            System.out.print("Deseja usar este resultado como novo DataFrame? (s/n): ");
            if (scanner.nextLine().toLowerCase().startsWith("s")) {
                dataFrame = resultDF;
                System.out.println("DataFrame atualizado!");
            }
        } catch (Exception e) {
            System.out.println("Erro ao aplicar agregação: " + e.getMessage());
        }
    }


    private void transformData(Scanner scanner) {
        if (!checkDataFrameLoaded()) return;

        System.out.println("Transformações disponíveis:");
        System.out.println("1. Selecionar colunas");
        System.out.println("2. Renomear coluna");
        System.out.println("3. Criar nova coluna");
        System.out.println("4. Ordenar dados");
        System.out.println("5. Remover duplicatas");
        System.out.println("6. Remover valores nulos");

        System.out.print("Digite o número da transformação: ");
        int choice = scanner.nextInt();
        scanner.nextLine(); // Limpa o buffer

        try {
            switch (choice) {
                case 1:
                    selectColumns(scanner);
                    break;
                case 2:
                    renameColumn(scanner);
                    break;
                case 3:
                    createNewColumn(scanner);
                    break;
                case 4:
                    sortData(scanner);
                    break;
                case 5:
                    removeDuplicates(scanner);
                    break;
                case 6:
                    removeNulls(scanner);
                    break;
                default:
                    System.out.println("Opção inválida!");
            }
        } catch (Exception e) {
            System.out.println("Erro na transformação: " + e.getMessage());
        }
    }


    private void selectColumns(Scanner scanner) {
        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite as colunas a selecionar (separadas por vírgula): ");
        String columnsInput = scanner.nextLine();

        String[] columns = columnsInput.split(",");
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i].trim();
        }

        dataFrame = dataFrame.select(columns[0], Arrays.copyOfRange(columns, 1, columns.length));
        System.out.println("Colunas selecionadas. Novo schema:");
        dataFrame.printSchema();
    }


    private void renameColumn(Scanner scanner) {
        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite o nome da coluna a renomear: ");
        String oldName = scanner.nextLine();

        if (!Arrays.asList(dataFrame.columns()).contains(oldName)) {
            System.out.println("Coluna não encontrada!");
            return;
        }

        System.out.print("Digite o novo nome: ");
        String newName = scanner.nextLine();

        dataFrame = dataFrame.withColumnRenamed(oldName, newName);
        System.out.println("Coluna renomeada. Novo schema:");
        dataFrame.printSchema();
    }

    /**
     * Cria uma nova coluna baseada em uma expressão
     */
    private void createNewColumn(Scanner scanner) {
        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite o nome para a nova coluna: ");
        String newColumnName = scanner.nextLine();

        System.out.println("Tipos de expressão:");
        System.out.println("1. Operação aritmética (coluna1 + coluna2)");
        System.out.println("2. Valor constante");
        System.out.println("3. Função de texto (upper, lower)");

        System.out.print("Digite o número do tipo de expressão: ");
        int expressionType = scanner.nextInt();
        scanner.nextLine(); // Limpa o buffer

        switch (expressionType) {
            case 1:
                System.out.print("Digite o nome da primeira coluna: ");
                String col1 = scanner.nextLine();

                System.out.print("Digite o operador (+, -, *, /): ");
                String operator = scanner.nextLine();

                System.out.print("Digite o nome da segunda coluna: ");
                String col2 = scanner.nextLine();

                switch (operator) {
                    case "+":
                        dataFrame = dataFrame.withColumn(newColumnName, col(col1).plus(col(col2)));
                        break;
                    case "-":
                        dataFrame = dataFrame.withColumn(newColumnName, col(col1).minus(col(col2)));
                        break;
                    case "*":
                        dataFrame = dataFrame.withColumn(newColumnName, col(col1).multiply(col(col2)));
                        break;
                    case "/":
                        dataFrame = dataFrame.withColumn(newColumnName, col(col1).divide(col(col2)));
                        break;
                    default:
                        System.out.println("Operador inválido!");
                        return;
                }
                break;

            case 2:
                System.out.print("Digite o valor constante: ");
                String constantValue = scanner.nextLine();

                dataFrame = dataFrame.withColumn(newColumnName, lit(constantValue));
                break;

            case 3:
                System.out.print("Digite o nome da coluna: ");
                String column = scanner.nextLine();

                System.out.print("Digite a função (upper/lower): ");
                String function = scanner.nextLine().toLowerCase();

                if (function.equals("upper")) {
                    dataFrame = dataFrame.withColumn(newColumnName, upper(col(column)));
                } else if (function.equals("lower")) {
                    dataFrame = dataFrame.withColumn(newColumnName, lower(col(column)));
                } else {
                    System.out.println("Função inválida!");
                    return;
                }
                break;

            default:
                System.out.println("Tipo de expressão inválido!");
                return;
        }

        System.out.println("Nova coluna criada. Amostra dos dados:");
        dataFrame.select(newColumnName).show(5);
    }


    private void sortData(Scanner scanner) {
        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite as colunas para ordenar (separadas por vírgula): ");
        String columnsInput = scanner.nextLine();

        String[] columns = columnsInput.split(",");
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i].trim();

            if (!Arrays.asList(dataFrame.columns()).contains(columns[i])) {
                System.out.println("Coluna não encontrada: " + columns[i]);
                return;
            }
        }

        System.out.print("Ordem ascendente? (s/n): ");
        boolean ascending = scanner.nextLine().toLowerCase().startsWith("s");

        if (ascending) {
            dataFrame = dataFrame.orderBy(Arrays.toString(columns));
        } else {
            // Criar colunas descendentes
            org.apache.spark.sql.Column[] sortCols = new org.apache.spark.sql.Column[columns.length];
            for (int i = 0; i < columns.length; i++) {
                sortCols[i] = col(columns[i]).desc();
            }
            dataFrame = dataFrame.orderBy(sortCols);
        }

        System.out.println("Dados ordenados. Amostra:");
        dataFrame.show(10);
    }


    private void removeDuplicates(Scanner scanner) {
        System.out.print("Remover considerando todas as colunas? (s/n): ");
        boolean allColumns = scanner.nextLine().toLowerCase().startsWith("s");

        long beforeCount = dataFrame.count();

        if (allColumns) {
            dataFrame = dataFrame.distinct();
        } else {
            System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

            System.out.print("Digite as colunas a considerar (separadas por vírgula): ");
            String columnsInput = scanner.nextLine();

            String[] columns = columnsInput.split(",");
            for (int i = 0; i < columns.length; i++) {
                columns[i] = columns[i].trim();
            }

            dataFrame = dataFrame.dropDuplicates(columns);
        }

        long afterCount = dataFrame.count();
        System.out.println("Duplicatas removidas: " + (beforeCount - afterCount) + " linhas");
    }


    private void removeNulls(Scanner scanner) {
        System.out.print("Remover se qualquer coluna tiver nulo? (s/n): ");
        boolean anyNull = scanner.nextLine().toLowerCase().startsWith("s");

        long beforeCount = dataFrame.count();

        if (anyNull) {
            dataFrame = dataFrame.na().drop();
        } else {
            System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

            System.out.print("Digite as colunas a considerar (separadas por vírgula): ");
            String columnsInput = scanner.nextLine();

            String[] columns = columnsInput.split(",");
            for (int i = 0; i < columns.length; i++) {
                columns[i] = columns[i].trim();
            }

            dataFrame = dataFrame.na().drop("all", columns);
        }

        long afterCount = dataFrame.count();
        System.out.println("Linhas com nulos removidas: " + (beforeCount - afterCount) + " linhas");
    }


    private void saveResults(Scanner scanner) {
        if (!checkDataFrameLoaded()) return;

        System.out.println("Formatos disponíveis:");
        System.out.println("1. CSV");
        System.out.println("2. Parquet");
        System.out.println("3. JSON");

        System.out.print("Digite o número do formato: ");
        int formatChoice = scanner.nextInt();
        scanner.nextLine(); // Limpa o buffer

        System.out.print("Digite o caminho para salvar: ");
        String path = scanner.nextLine();

        System.out.print("Sobrescrever se existir? (s/n): ");
        boolean overwrite = scanner.nextLine().toLowerCase().startsWith("s");

        try {
            switch (formatChoice) {
                case 1:
                    dataFrame.write().option("header", "true")
                            .mode(overwrite ? "overwrite" : "errorIfExists")
                            .csv(path);
                    break;
                case 2:
                    dataFrame.write()
                            .mode(overwrite ? "overwrite" : "errorIfExists")
                            .parquet(path);
                    break;
                case 3:
                    dataFrame.write()
                            .mode(overwrite ? "overwrite" : "errorIfExists")
                            .json(path);
                    break;
                default:
                    System.out.println("Formato inválido!");
                    return;
            }

            System.out.println("Dados salvos com sucesso em: " + path);
        } catch (Exception e) {
            System.out.println("Erro ao salvar os dados: " + e.getMessage());
        }
    }

    /**
     * Verifica se um DataFrame foi carregado
     */
    private boolean checkDataFrameLoaded() {
        if (dataFrame == null) {
            System.out.println("Nenhum dado carregado. Por favor, carregue um arquivo primeiro.");
            return false;
        }
        return true;
    }

    /**
     * Encerra a sessão Spark
     */
    private void shutdown() {
        if (spark != null) {
            spark.stop();
            System.out.println("Sessão Spark encerrada. Até mais!");
        }
    }
}