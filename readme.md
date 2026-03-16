# Java Spark Data Analyzer

<div align="center">

![Java](https://img.shields.io/badge/Java-21-orange.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.3-blue.svg)
[![License](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)](http://creativecommons.org/licenses/by-nc-sa/4.0/)

</div>

Ferramenta CLI interativa para análise exploratória de dados utilizando **Apache Spark 3.5** e **Java 21**.

Carregue arquivos CSV, Parquet ou JSON, explore schemas, aplique filtros e transformações, execute SQL, realize joins, perfils de dados e exporte os resultados — tudo sem escrever uma linha de código.

---

## Funcionalidades

- **Carregamento multi-formato**: CSV, Parquet e JSON com inferência automática de schema
- **Exploração**: schema, amostra e estatísticas descritivas (com contagem de nulos por coluna)
- **Filtros** com operadores `=`, `>`, `<`, `>=`, `<=`, `!=`
- **Agregações** (`avg`, `sum`, `min`, `max`, `count`) com `GROUP BY` opcional
- **Transformações**: seleção, renomear, criar colunas via expressão SQL, ordenar, remover duplicatas/nulos, cast de tipos, preenchimento de nulos, amostragem
- **Funções de janela**: `RANK`, `DENSE_RANK`, `ROW_NUMBER`, `LAG`, `LEAD`
- **SQL interativo**: execute queries SQL diretamente sobre o DataFrame carregado
- **Joins**: INNER, LEFT, RIGHT e FULL OUTER com chave simples ou múltiplas colunas
- **Data Profiler**: análise completa por coluna (tipo, nulos, distintos, min/max)
- **Histórico de operações**: rastreamento de todas as transformações aplicadas
- **Exportação**: CSV, Parquet e JSON com suporte a arquivo único (coalesce)

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
│   ├── DataLoader.java            CSV / Parquet / JSON loading
│   ├── DataTransformer.java       Stateless transformations + window functions
│   ├── DataAggregator.java        Stateless aggregations
│   ├── DataExporter.java          CSV / Parquet / JSON export
│   ├── SqlExecutor.java           Spark SQL query execution
│   ├── DataJoiner.java            DataFrame join operations
│   ├── DataProfiler.java          Per-column data profiling
│   ├── AggFunction.java           Aggregation function enum
│   ├── ExportFormat.java          Export format enum
│   ├── JoinType.java              Join type enum
│   ├── ColumnType.java            Column type enum
│   └── WindowFunctionType.java    Window function enum
├── ui/
│   ├── MenuRenderer.java          Renders all CLI menus
│   └── InputReader.java           Typed input reading
└── util/
    ├── OperationHistory.java       Operation history tracking
    ├── ProgressBar.java            Terminal progress bar
    └── SchemaValidator.java        DataFrame schema helpers
```

**Princípio central:** todas as classes `core/` são completamente stateless — recebem e retornam `Dataset<Row>`. O estado (DataFrame atual) é mantido apenas pela `UserInterface`.

---

## Requisitos

| Componente | Versão mínima |
|------------|--------------|
| Java       | 21           |
| Maven      | 3.8+         |

---

## Como executar

### Script (recomendado)

```bash
./run.sh
```

Compila o projeto e executa em seguida, já com todas as flags JVM necessárias para o Spark 3.x.

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
 2. Schema e colunas
 3. Amostra de dados
 4. Estatísticas descritivas
 5. Filtrar dados
 6. Agregar dados
 7. Transformar dados
 8. Exportar dados
 9. Executar SQL
10. Join com outro dataset
11. Data Profiler
12. Histórico de operações
13. Sair
=================================
Escolha (1-13): 1

Formato (1-CSV, 2-Parquet, 3-JSON): 1
Caminho do arquivo: dados_vendas.csv
Dados carregados! Linhas: 100 | Colunas: 10

Escolha (1-13): 9
Query SQL (use 'df' como nome da tabela):
> SELECT Categoria, SUM(Preco * Quantidade) AS total FROM df GROUP BY Categoria ORDER BY total DESC

+-----------+----------+
|Categoria  |total     |
+-----------+----------+
|Eletrônicos|125430.00 |
|Móveis     |67890.50  |
...
```

---

## Tecnologias

| Tecnologia      | Versão | Uso                                |
|-----------------|--------|------------------------------------|
| Apache Spark    | 3.5.3  | Processamento distribuído de dados |
| Scala (runtime) | 2.12   | Runtime do Spark                   |
| JUnit Jupiter   | 5.10   | Testes de integração               |
| JaCoCo          | 0.8.10 | Cobertura de testes                |
| SLF4J           | —      | Logging estruturado (via Spark)    |

---

## Licença

Este projeto está licenciado sob a [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International](http://creativecommons.org/licenses/by-nc-sa/4.0/).

[![License](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)](http://creativecommons.org/licenses/by-nc-sa/4.0/)
