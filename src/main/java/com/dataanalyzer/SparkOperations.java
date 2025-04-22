package com.dataanalyzer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

import java.io.File;
import java.util.Arrays;
import java.util.Scanner;

public class SparkOperations {

    private SparkSession spark;
    private Dataset<Row> dataFrame;

    public SparkOperations(SparkSession spark) {
        this.spark = spark;
        this.dataFrame = null; // Inicializa como nulo
    }

    public void loadData(Scanner scanner) {
        System.out.print("Digite o caminho para o arquivo CSV (ou 'example' para usar o arquivo de exemplo): ");
        String path = scanner.nextLine();

        if (path.equalsIgnoreCase("example")) {
            path = "src/main/resources/dados_vendas.csv";
            System.out.println("Usando o arquivo de exemplo: " + path);
        }

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
            System.out.println("Carregando dados...");
            ProgressBar progressBar = new ProgressBar(); // Supondo que ProgressBar está no mesmo pacote ou importado

            // Simulação de progresso
            for (int i = 0; i <= 20; i++) {
                progressBar.update(i / 20.0, "Inicializando...");
                Thread.sleep(50);
            }

            Dataset<Row> tempDF = spark.read()
                    .option("header", hasHeader)
                    .option("delimiter", delimiter)
                    .option("inferSchema", "true") // Importante para inferir tipos
                    .csv(path);

            // Simulação de progresso
            for (int i = 21; i <= 80; i++) {
                progressBar.update(i / 100.0, "Processando schema...");
                Thread.sleep(20);
            }

            tempDF.cache(); // Otimiza o desempenho para ações futuras
            long rowCount = tempDF.count(); // Ação para materializar o DF e obter contagem

            // Simulação de progresso
            for (int i = 81; i <= 100; i++) {
                progressBar.update(i / 100.0, "Finalizando...");
                Thread.sleep(10);
            }

            progressBar.complete();

            this.dataFrame = tempDF; // Armazena o DataFrame carregado

            System.out.println("Dados carregados com sucesso!");
            System.out.println("Número de linhas: " + rowCount);
            System.out.println("Número de colunas: " + dataFrame.columns().length);
        } catch (Exception e) {
            System.out.println("Erro ao carregar os dados: " + e.getMessage());
            // e.printStackTrace(); // Útil para debug
        }
    }

    public void showSchema() {
        if (checkDataFrameLoaded()) {
            System.out.println("Schema do DataFrame:");
            dataFrame.printSchema();
        }
    }

    public void showSample() {
        if (checkDataFrameLoaded()) {
            System.out.println("Amostra de dados (20 primeiras linhas):");
            dataFrame.show(20, false); // false para não truncar colunas largas
        }
    }

    public void showStats() {
        if (checkDataFrameLoaded()) {
            System.out.println("Estatísticas descritivas:");

            ProgressBar progressBar = new ProgressBar();
            System.out.println("Calculando estatísticas...");

            try {
                // Simulação de progresso
                for (int i = 0; i <= 50; i++) {
                    progressBar.update(i / 100.0, "Preparando dados...");
                    Thread.sleep(10);
                }

                Dataset<Row> statsDF = dataFrame.describe(); // Calcula estatísticas básicas

                // Simulação de progresso
                for (int i = 51; i <= 100; i++) {
                    progressBar.update(i / 100.0, "Processando métricas...");
                    Thread.sleep(10);
                }

                progressBar.complete();

                statsDF.show();

                System.out.println("Contagem de valores nulos por coluna:");
                for (String col : dataFrame.columns()) {
                    long nullCount = dataFrame.filter(col(col).isNull()).count();
                    System.out.println(col + ": " + nullCount);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Boa prática restaurar o status de interrupção
                System.out.println("Operação interrompida: " + e.getMessage());
            } catch (Exception e) {
                 System.out.println("Erro ao calcular estatísticas: " + e.getMessage());
            }
        }
    }

    public void filterData(Scanner scanner) {
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
            System.out.println("Aplicando filtro...");
            ProgressBar progressBar = new ProgressBar();

            // Simulação de progresso
            for (int i = 0; i <= 50; i++) {
                progressBar.update(i / 100.0, "Preparando filtro...");
                Thread.sleep(10);
            }

            Dataset<Row> filteredDF;
            // Usando functions.col para clareza e evitar ambiguidade
            org.apache.spark.sql.Column colRef = functions.col(column);

            // Tentativa de converter valor para tipo numérico se necessário
            // Isso é uma simplificação, idealmente verificaríamos o tipo da coluna
            Object filterValue;
            try {
                filterValue = Double.parseDouble(value);
            } catch (NumberFormatException e) {
                filterValue = value; // Mantém como string se não for número
            }

            switch (operator) {
                case "=":
                    filteredDF = dataFrame.filter(colRef.equalTo(filterValue));
                    break;
                case ">":
                    filteredDF = dataFrame.filter(colRef.gt(filterValue));
                    break;
                case "<":
                    filteredDF = dataFrame.filter(colRef.lt(filterValue));
                    break;
                case ">=":
                    filteredDF = dataFrame.filter(colRef.geq(filterValue));
                    break;
                case "<=":
                    filteredDF = dataFrame.filter(colRef.leq(filterValue));
                    break;
                case "!=":
                    filteredDF = dataFrame.filter(colRef.notEqual(filterValue));
                    break;
                default:
                    progressBar.complete("Operador inválido!");
                    return;
            }

            // Simulação de progresso
            for (int i = 51; i <= 90; i++) {
                progressBar.update(i / 100.0, "Executando filtro...");
                Thread.sleep(10);
            }

            long resultCount = filteredDF.count(); // Ação para executar o filtro

            // Simulação de progresso
            for (int i = 91; i <= 100; i++) {
                progressBar.update(i / 100.0, "Finalizando...");
                Thread.sleep(5);
            }

            progressBar.complete();

            this.dataFrame = filteredDF; // Atualiza o DataFrame principal

            System.out.println("Filtro aplicado! Número de linhas após filtro: " + resultCount);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Operação interrompida: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Erro ao aplicar filtro: " + e.getMessage());
            // e.printStackTrace();
        }
    }

    public void aggregateData(Scanner scanner) {
        if (!checkDataFrameLoaded()) return;

        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite a coluna para agrupar (deixe em branco para não agrupar): ");
        String groupByColumn = scanner.nextLine().trim();

        System.out.print("Digite a coluna para agregar: ");
        String aggregateColumn = scanner.nextLine().trim();

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
        int functionChoice;
         try {
            functionChoice = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException e) {
            System.out.println("Entrada inválida. Por favor, digite um número.");
            return;
        }

        try {
            System.out.println("Executando agregação...");
            ProgressBar progressBar = new ProgressBar();

            // Simulação de progresso
            for (int i = 0; i <= 40; i++) {
                progressBar.update(i / 100.0, "Preparando dados...");
                Thread.sleep(15);
            }

            Dataset<Row> resultDF;
            String operationName = "";
            String resultColName = "";

            org.apache.spark.sql.RelationalGroupedDataset groupedData = null;
            if (!groupByColumn.isEmpty()) {
                groupedData = dataFrame.groupBy(col(groupByColumn));
            }

            switch (functionChoice) {
                case 1:
                    resultColName = "avg_" + aggregateColumn;
                    operationName = "Calculando média";
                    resultDF = (groupedData != null)
                        ? groupedData.agg(avg(aggregateColumn).alias(resultColName)).orderBy(col(groupByColumn))
                        : dataFrame.agg(avg(aggregateColumn).alias(resultColName));
                    break;
                case 2:
                    resultColName = "sum_" + aggregateColumn;
                    operationName = "Calculando soma";
                     resultDF = (groupedData != null)
                        ? groupedData.agg(sum(aggregateColumn).alias(resultColName)).orderBy(col(groupByColumn))
                        : dataFrame.agg(sum(aggregateColumn).alias(resultColName));
                    break;
                case 3:
                    resultColName = "min_" + aggregateColumn;
                    operationName = "Encontrando mínimo";
                     resultDF = (groupedData != null)
                        ? groupedData.agg(min(aggregateColumn).alias(resultColName)).orderBy(col(groupByColumn))
                        : dataFrame.agg(min(aggregateColumn).alias(resultColName));
                    break;
                case 4:
                    resultColName = "max_" + aggregateColumn;
                    operationName = "Encontrando máximo";
                     resultDF = (groupedData != null)
                        ? groupedData.agg(max(aggregateColumn).alias(resultColName)).orderBy(col(groupByColumn))
                        : dataFrame.agg(max(aggregateColumn).alias(resultColName));
                    break;
                case 5:
                    resultColName = "count_" + aggregateColumn;
                    operationName = "Contando registros";
                     resultDF = (groupedData != null)
                        ? groupedData.agg(count(aggregateColumn).alias(resultColName)).orderBy(col(groupByColumn))
                        : dataFrame.agg(count(aggregateColumn).alias(resultColName));
                    break;
                default:
                    progressBar.complete("Função inválida!");
                    return;
            }

             // Simulação de progresso
            for (int i = 41; i <= 80; i++) {
                progressBar.update(i / 100.0, operationName + "...");
                Thread.sleep(10);
            }

            resultDF.cache(); // Otimiza para a exibição e possível reutilização
            resultDF.count(); // Ação para executar a agregação

             // Simulação de progresso
            for (int i = 81; i <= 100; i++) {
                progressBar.update(i / 100.0, "Finalizando...");
                Thread.sleep(5);
            }

            progressBar.complete();

            System.out.println("Resultado da agregação:");
            resultDF.show(20, false);

            System.out.print("Deseja usar este resultado como novo DataFrame? (s/n): ");
            if (scanner.nextLine().toLowerCase().startsWith("s")) {
                this.dataFrame = resultDF;
                 // Se agrupou, o schema mudou
                 if (!groupByColumn.isEmpty()) {
                    this.dataFrame = this.dataFrame.withColumnRenamed(groupByColumn, groupByColumn); // Renomeia para si mesmo para evitar problemas com alias
                 }
                System.out.println("DataFrame atualizado!");
                showSchema(); // Mostra o novo schema
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Operação interrompida: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Erro ao aplicar agregação: " + e.getMessage());
            // e.printStackTrace();
        }
    }

     public void transformData(Scanner scanner) {
        if (!checkDataFrameLoaded()) return;

        System.out.println("\nTransformações disponíveis:");
        System.out.println("1. Selecionar colunas");
        System.out.println("2. Renomear coluna");
        System.out.println("3. Criar nova coluna (Expressão)");
        System.out.println("4. Ordenar dados");
        System.out.println("5. Remover duplicatas");
        System.out.println("6. Remover valores nulos (linhas)");
        System.out.println("7. Voltar");

        System.out.print("Digite o número da transformação: ");
         int choice;
         try {
            choice = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException e) {
            System.out.println("Entrada inválida. Por favor, digite um número.");
            return;
        }

        try {
            switch (choice) {
                case 1:
                    selectColumns(scanner);
                    break;
                case 2:
                    renameColumn(scanner);
                    break;
                case 3:
                    createNewColumnExpr(scanner); // Renomeado para clareza
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
                case 7:
                    return; // Volta ao menu principal
                default:
                    System.out.println("Opção inválida!");
            }
            // Mostra o schema atualizado após a transformação (exceto voltar)
            if (choice != 7 && checkDataFrameLoaded()) {
                 System.out.println("\nSchema atual do DataFrame:");
                 showSchema();
                 System.out.println("Amostra atual do DataFrame:");
                 showSample();
            }
        } catch (Exception e) {
            System.out.println("Erro na transformação: " + e.getMessage());
            // e.printStackTrace();
        }
    }

    private void selectColumns(Scanner scanner) {
        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite as colunas a selecionar (separadas por vírgula): ");
        String columnsInput = scanner.nextLine();

        String[] columns = Arrays.stream(columnsInput.split(","))
                               .map(String::trim)
                               .filter(s -> !s.isEmpty())
                               .toArray(String[]::new);

        if (columns.length == 0) {
            System.out.println("Nenhuma coluna válida especificada.");
            return;
        }

        // Verifica se todas as colunas existem
        for (String col : columns) {
            if (!Arrays.asList(dataFrame.columns()).contains(col)) {
                System.out.println("Coluna não encontrada: " + col);
                return;
            }
        }

        try {
             if (columns.length == 1) {
                 dataFrame = dataFrame.select(columns[0]);
             } else {
                 // Converte array para Seq para o select
                 // org.apache.spark.sql.Column[] colsToSelect = Arrays.stream(columns).map(functions::col).toArray(org.apache.spark.sql.Column[]::new);
                 // dataFrame = dataFrame.select(colsToSelect);
                 // Ou mais simples para strings:
                  dataFrame = dataFrame.select(columns[0], Arrays.copyOfRange(columns, 1, columns.length));
             }
            System.out.println("Colunas selecionadas.");
        } catch (Exception e) {
             System.out.println("Erro ao selecionar colunas: " + e.getMessage());
        }
    }

    private void renameColumn(Scanner scanner) {
        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite o nome da coluna a renomear: ");
        String oldName = scanner.nextLine().trim();

        if (!Arrays.asList(dataFrame.columns()).contains(oldName)) {
            System.out.println("Coluna não encontrada!");
            return;
        }

        System.out.print("Digite o novo nome: ");
        String newName = scanner.nextLine().trim();

        if (newName.isEmpty()) {
            System.out.println("Novo nome não pode ser vazio.");
            return;
        }
        if (Arrays.asList(dataFrame.columns()).contains(newName)) {
             System.out.println("Já existe uma coluna com o nome: " + newName);
             return;
        }

        try {
            dataFrame = dataFrame.withColumnRenamed(oldName, newName);
            System.out.println("Coluna '" + oldName + "' renomeada para '" + newName + "'.");
        } catch (Exception e) {
            System.out.println("Erro ao renomear coluna: " + e.getMessage());
        }
    }

    // Renomeado para createNewColumnExpr para usar Spark SQL expressions
    private void createNewColumnExpr(Scanner scanner) {
         System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

         System.out.print("Digite o nome para a nova coluna: ");
         String newColumnName = scanner.nextLine().trim();

          if (newColumnName.isEmpty()) {
            System.out.println("Nome da nova coluna não pode ser vazio.");
            return;
        }
        if (Arrays.asList(dataFrame.columns()).contains(newColumnName)) {
             System.out.println("Já existe uma coluna com o nome: " + newColumnName);
             return;
        }

         System.out.println("Digite a expressão SQL para definir a nova coluna.");
         System.out.println("Exemplos:");
         System.out.println("  `coluna1 + coluna2`");
         System.out.println("  `coluna1 * 10`");
         System.out.println("  `upper(coluna_texto)`");
         System.out.println("  `cast(coluna_string as int)`");
         System.out.println("  `case when coluna > 10 then 'Alto' else 'Baixo' end`");
         System.out.println("  `'valor_literal'` (com aspas simples)");
         System.out.print("Expressão: ");
         String expression = scanner.nextLine();

         if (expression.isEmpty()) {
             System.out.println("Expressão não pode ser vazia.");
             return;
         }

         try {
            dataFrame = dataFrame.withColumn(newColumnName, expr(expression));
            System.out.println("Nova coluna '" + newColumnName + "' criada com a expressão.");
            System.out.println("Amostra da nova coluna:");
            dataFrame.select(newColumnName).show(5, false);
         } catch (Exception e) {
            System.out.println("Erro ao criar nova coluna com expressão: " + e.getMessage());
            System.out.println("Verifique a sintaxe da expressão e os nomes das colunas.");
            // Tenta remover a coluna caso tenha sido criada parcialmente com erro (pode não funcionar sempre)
            if (Arrays.asList(dataFrame.columns()).contains(newColumnName)) {
                dataFrame = dataFrame.drop(newColumnName);
            }
        }
    }


    private void sortData(Scanner scanner) {
        System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));

        System.out.print("Digite as colunas para ordenar (separadas por vírgula): ");
        String columnsInput = scanner.nextLine();

        String[] columnNames = Arrays.stream(columnsInput.split(","))
                                   .map(String::trim)
                                   .filter(s -> !s.isEmpty())
                                   .toArray(String[]::new);

        if (columnNames.length == 0) {
            System.out.println("Nenhuma coluna válida especificada.");
            return;
        }

        org.apache.spark.sql.Column[] sortColumns = new org.apache.spark.sql.Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            if (!Arrays.asList(dataFrame.columns()).contains(columnNames[i])) {
                System.out.println("Coluna não encontrada: " + columnNames[i]);
                return;
            }
            sortColumns[i] = col(columnNames[i]);
        }

        System.out.print("Ordem ascendente para todas? (s/n): ");
        boolean ascending = scanner.nextLine().toLowerCase().startsWith("s");

        try {
            if (ascending) {
                 dataFrame = dataFrame.orderBy(sortColumns); // Usa as colunas diretamente
            } else {
                // Se descendente, aplica .desc() a cada coluna
                org.apache.spark.sql.Column[] descSortColumns = Arrays.stream(sortColumns)
                                                                    .map(org.apache.spark.sql.Column::desc)
                                                                    .toArray(org.apache.spark.sql.Column[]::new);
                dataFrame = dataFrame.orderBy(descSortColumns);
            }

            System.out.println("Dados ordenados. Amostra:");
            dataFrame.show(10, false);
        } catch (Exception e) {
            System.out.println("Erro ao ordenar dados: " + e.getMessage());
        }
    }

    private void removeDuplicates(Scanner scanner) {
        long beforeCount = dataFrame.count();
        System.out.println("Número atual de linhas: " + beforeCount);

        System.out.print("Remover duplicatas exatas (considerando todas as colunas)? (s/n): ");
        boolean allColumns = scanner.nextLine().toLowerCase().startsWith("s");

        try {
            if (allColumns) {
                dataFrame = dataFrame.distinct();
                System.out.println("Removendo duplicatas exatas...");
            } else {
                System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));
                System.out.print("Digite as colunas chave para identificar duplicatas (separadas por vírgula): ");
                String columnsInput = scanner.nextLine();

                String[] columns = Arrays.stream(columnsInput.split(","))
                                           .map(String::trim)
                                           .filter(s -> !s.isEmpty())
                                           .toArray(String[]::new);

                 if (columns.length == 0) {
                    System.out.println("Nenhuma coluna chave especificada.");
                    return;
                 }

                 // Verifica se todas as colunas existem
                for (String col : columns) {
                    if (!Arrays.asList(dataFrame.columns()).contains(col)) {
                        System.out.println("Coluna não encontrada: " + col);
                        return;
                    }
                }

                System.out.println("Removendo linhas duplicadas com base nas colunas: " + String.join(", ", columns) + "...");
                dataFrame = dataFrame.dropDuplicates(columns);
            }

            long afterCount = dataFrame.count();
            System.out.println("Duplicatas removidas: " + (beforeCount - afterCount) + " linhas removidas.");
            System.out.println("Número final de linhas: " + afterCount);
        } catch (Exception e) {
            System.out.println("Erro ao remover duplicatas: " + e.getMessage());
            // Restaurar pode ser complexo, melhor informar o erro
            dataFrame = spark.emptyDataFrame(); // Ou invalidar o estado
            System.out.println("Estado do DataFrame pode estar inválido devido ao erro.");
        }
    }

    private void removeNulls(Scanner scanner) {
         long beforeCount = dataFrame.count();
         System.out.println("Número atual de linhas: " + beforeCount);

        System.out.print("Remover linhas onde *qualquer* coluna tem valor nulo? (s/n): ");
        boolean dropAny = scanner.nextLine().toLowerCase().startsWith("s");

        try {
            if (dropAny) {
                System.out.println("Removendo linhas com qualquer valor nulo...");
                dataFrame = dataFrame.na().drop("any"); // 'any' é o padrão, mas explícito é bom
            } else {
                System.out.print("Remover linhas onde *todas* as colunas especificadas são nulas? (s/n): ");
                 boolean dropAll = scanner.nextLine().toLowerCase().startsWith("s");
                 String mode = dropAll ? "all" : "any"; // 'any' aqui significa nulo em pelo menos uma das cols especificadas

                System.out.println("Colunas disponíveis: " + String.join(", ", dataFrame.columns()));
                System.out.print("Digite as colunas a considerar para verificar nulos (separadas por vírgula, deixe em branco para todas): ");
                String columnsInput = scanner.nextLine();

                String[] columns = Arrays.stream(columnsInput.split(","))
                                           .map(String::trim)
                                           .filter(s -> !s.isEmpty())
                                           .toArray(String[]::new);

                if (columns.length > 0) {
                     // Verifica se todas as colunas existem
                    for (String col : columns) {
                        if (!Arrays.asList(dataFrame.columns()).contains(col)) {
                            System.out.println("Coluna não encontrada: " + col);
                            return;
                        }
                    }
                    System.out.println("Removendo linhas com valores nulos ('"+ mode +"') nas colunas: " + String.join(", ", columns) + "...");
                    dataFrame = dataFrame.na().drop(mode, columns);
                } else {
                    // Se não especificou colunas, aplica a todas (igual a dropAny inicial)
                    System.out.println("Nenhuma coluna especificada, removendo linhas onde *qualquer* coluna é nula...");
                    dataFrame = dataFrame.na().drop("any");
                }
            }

            long afterCount = dataFrame.count();
            System.out.println("Linhas com nulos removidas: " + (beforeCount - afterCount) + " linhas removidas.");
            System.out.println("Número final de linhas: " + afterCount);
         } catch (Exception e) {
            System.out.println("Erro ao remover nulos: " + e.getMessage());
             dataFrame = spark.emptyDataFrame(); // Ou invalidar o estado
             System.out.println("Estado do DataFrame pode estar inválido devido ao erro.");
        }
    }

    public void saveResults(Scanner scanner) {
        if (!checkDataFrameLoaded()) return;

        System.out.println("Formatos disponíveis:");
        System.out.println("1. CSV");
        System.out.println("2. Parquet");
        System.out.println("3. JSON");

        System.out.print("Digite o número do formato: ");
         int formatChoice;
         try {
            formatChoice = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException e) {
            System.out.println("Entrada inválida. Por favor, digite um número.");
            return;
        }

        System.out.print("Digite o caminho (diretório) para salvar: ");
        String path = scanner.nextLine().trim();

        if (path.isEmpty()) {
            System.out.println("Caminho não pode ser vazio.");
            return;
        }

        System.out.print("Sobrescrever diretório se existir? (s/n): ");
        boolean overwrite = scanner.nextLine().toLowerCase().startsWith("s");
        String saveMode = overwrite ? "overwrite" : "errorifexists"; // Spark usa 'errorifexists'

        try {
            System.out.println("Salvando dados em " + path + "...");
            ProgressBar progressBar = new ProgressBar();

             // Simulação de progresso (salvar pode ser rápido ou demorado)
            for (int i = 0; i <= 90; i++) {
                progressBar.update(i / 100.0, "Gravando arquivo(s)...");
                Thread.sleep(10); // Simula I/O
            }

            org.apache.spark.sql.DataFrameWriter<Row> writer = dataFrame.write().mode(saveMode);

            switch (formatChoice) {
                case 1: // CSV
                    System.out.print("Incluir cabeçalho? (s/n): ");
                    boolean includeHeader = scanner.nextLine().toLowerCase().startsWith("s");
                    System.out.print("Delimitador (padrão ','): ");
                    String delimiter = scanner.nextLine();
                    if (delimiter.isEmpty()) delimiter = ",";

                    writer.option("header", includeHeader)
                          .option("delimiter", delimiter)
                          .csv(path);
                    break;
                case 2: // Parquet (eficiente, preserva schema)
                    writer.parquet(path);
                    break;
                case 3: // JSON (cada linha um objeto JSON)
                    writer.json(path);
                    break;
                default:
                    progressBar.complete("Formato inválido!");
                    return;
            }

            // Simulação de progresso final
            for (int i = 91; i <= 100; i++) {
                progressBar.update(i / 100.0, "Finalizando...");
                Thread.sleep(5);
            }

            progressBar.complete();

            System.out.println("Dados salvos com sucesso em: " + path);
        } catch (Exception e) {
            // Spark frequentemente lança exceções específicas, ex: AnalysisException se o diretório existe e modo é 'errorifexists'
            System.out.println("Erro ao salvar os dados: " + e.getMessage());
            // e.printStackTrace();
        }
    }

    private boolean checkDataFrameLoaded() {
        if (dataFrame == null) {
            System.out.println("Nenhum dado carregado. Por favor, carregue um arquivo primeiro (Opção 1).");
            return false;
        }
         if (dataFrame.isEmpty()) {
             System.out.println("O DataFrame está vazio (0 linhas). Carregue dados ou verifique filtros/transformações.");
            return false; // Ou permitir operações em DF vazio, dependendo do caso
        }
        return true;
    }

    // Opcional: Getter para o DataFrame se precisar externamente (improvável com esta estrutura)
    // public Dataset<Row> getDataFrame() {
    //     return dataFrame;
    // }
} 