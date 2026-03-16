# Java Spark Data Analyzer

<div align="center">

![Java](https://img.shields.io/badge/Java-11+-orange.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4.1-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

</div>

Ferramenta CLI interativa para análise exploratória de dados utilizando **Apache Spark 3.4** e **Java 11**.

Carregue arquivos CSV, explore schemas, aplique filtros e transformações, gere agregações e exporte os resultados — tudo sem escrever uma linha de código.

---

## Funcionalidades

- Carregamento de CSV com inferência automática de schema
- Visualização de schema, amostra e estatísticas descritivas (com contagem de nulos por coluna)
- Filtros com operadores `=`, `>`, `<`, `>=`, `<=`, `!=`
- Agregações (`avg`, `sum`, `min`, `max`, `count`) com `GROUP BY` opcional
- Transformações: seleção de colunas, renomear, criar colunas via expressão SQL, ordenar, remover duplicatas e nulos
- Exportação nos formatos **CSV**, **Parquet** e **JSON**

---

## Arquitetura

```
src/main/java/com/dataanalyzer/
├── DataAnalyzer.java              Entry point — wires all components
├── UserInterface.java             Interactive loop — holds DataFrame state
├── config/
│   └── SparkConfig.java           Reads application.properties
├── session/
│   └── SparkSessionManager.java   SparkSession lifecycle
├── core/
│   ├── DataLoader.java            CSV loading
│   ├── DataTransformer.java       Stateless transformations
│   ├── DataAggregator.java        Stateless aggregations
│   ├── DataExporter.java          CSV / Parquet / JSON export
│   ├── AggFunction.java           Aggregation function enum
│   └── ExportFormat.java          Export format enum
├── ui/
│   ├── MenuRenderer.java          Renders all CLI menus
│   └── InputReader.java           Typed input reading
└── util/
    ├── ProgressBar.java            Terminal progress bar
    └── SchemaValidator.java        DataFrame schema helpers
```

**Princípio central:** `DataTransformer` e `DataAggregator` são completamente stateless — recebem e retornam `Dataset<Row>`. O estado (DataFrame atual) é mantido apenas pela `UserInterface`.

---

## Requisitos

| Componente | Versão mínima |
|------------|--------------|
| Java       | 11           |
| Maven      | 3.8+         |

> **Java 17+**: adicione as flags JVM abaixo ao executar o JAR:
> ```
> --add-opens java.base/sun.nio.ch=ALL-UNNAMED
> --add-opens java.base/java.nio=ALL-UNNAMED
> --add-opens java.base/java.lang=ALL-UNNAMED
> --add-opens java.base/java.util=ALL-UNNAMED
> ```

---

## Como executar

### Com Maven (modo desenvolvimento)

```bash
mvn compile exec:java -Dexec.mainClass="com.dataanalyzer.DataAnalyzer"
```

### Gerando o JAR executável

```bash
mvn clean package -DskipTests
java -jar target/java-spark-data-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## Como rodar os testes

```bash
# Executa todos os testes
mvn test

# Gera relatório de cobertura (target/site/jacoco/index.html)
mvn verify
```

Os testes usam uma `SparkSession` real em modo local — não há mocks de Spark.

---

## Schema do arquivo de exemplo

O arquivo `src/main/resources/dados_vendas.csv` contém dados de vendas de eletrônicos:

| Coluna     | Tipo    | Descrição                     |
|------------|---------|-------------------------------|
| ID         | Integer | Identificador da venda        |
| Data       | String  | Data da venda (YYYY-MM-DD)    |
| Produto    | String  | Nome do produto               |
| Categoria  | String  | Categoria do produto          |
| Preco      | Double  | Preço unitário                |
| Quantidade | Integer | Quantidade vendida            |
| ClienteID  | Integer | Identificador do cliente      |
| Regiao     | String  | Região geográfica             |
| Vendedor   | String  | Nome do vendedor              |
| Desconto   | Double  | Percentual de desconto        |

---

## Exemplo de uso

```
=================================
       Java Data Analyzer
=================================
1. Carregar dados
...
Escolha (1-9): 1

Caminho do CSV (ou 'example' para dados de exemplo): example
Dados carregados! Linhas: 100 | Colunas: 10

Escolha (1-9): 6
Coluna para agrupar (Enter para não agrupar): Regiao
Coluna para agregar: Preco
Função (1-5): 2   ← SUM

--- Resultado da Agregação ---
+-------+----------+
|Regiao |sum_Preco |
+-------+----------+
|Norte  |45230.00  |
|Sul    |67890.50  |
...
```

---

## Solução de Problemas

### Windows e Hadoop

O Spark usa funcionalidades do Hadoop que podem gerar avisos no Windows. Se encontrar avisos relacionados ao `winutils.exe`:

1. Ignore-os (não afetam a funcionalidade)
2. Ou configure o ambiente: baixe o [winutils.exe](https://github.com/cdarlint/winutils), crie `C:\hadoop\bin`, coloque o arquivo lá e defina `HADOOP_HOME=C:\hadoop`

---

## Tecnologias

| Tecnologia      | Versão | Uso                                |
|-----------------|--------|------------------------------------|
| Apache Spark    | 3.4.1  | Processamento distribuído de dados |
| Scala (runtime) | 2.12   | Runtime do Spark                   |
| JUnit Jupiter   | 5.10   | Testes de integração               |
| JaCoCo          | 0.8.10 | Cobertura de testes                |
| SLF4J           | —      | Logging estruturado (via Spark)    |

---

## Licença

MIT
