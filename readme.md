# 🕵️ Java Spark Data Analyzer

<div align="center">

![Java](https://img.shields.io/badge/Java-11+-orange.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4.1-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**Um aplicativo Java de análise de dados com Apache Spark que compete diretamente com soluções em Python.**

</div>

---

## ✨ Principais Funcionalidades

- 📊 **Carregamento intuitivo de dados** - Suporte para CSV com diversas opções de configuração
- 🔍 **Visualização interativa** - Exibição de schemas, amostras e estatísticas descritivas
- 🔄 **Transformações poderosas** - Seleção de colunas, criação de novas colunas, renomeação e ordenação
- 🔎 **Filtros avançados** - Aplicação de condições para filtrar dados com precisão
- 📈 **Agregações flexíveis** - Funções como média, soma, mínimo, máximo e contagem
- ⚙️ **Tratamento de dados** - Remoção eficiente de duplicatas e valores nulos
- 💾 **Múltiplos formatos de exportação** - Salvamento em CSV, Parquet e JSON

## 📋 Requisitos

- Java 8 ou 11 (recomendado)
- Java 17+ (requer configurações adicionais)
- Apache Maven
- Memória suficiente para processar seus conjuntos de dados

## ⚙️ Configurações por Versão do Java

### Java 8 ou 11 (Recomendado)
Java 8 ou 11 funcionam diretamente sem configurações adicionais.

### Java 17+
Para usar com Java 17 ou superior, é necessário adicionar as seguintes opções JVM:
```bash
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
```

## 🔧 Instalação

1. Clone o repositório:
```bash
git clone https://github.com/bulletdev/java-spark-data-analyzer.git
cd java-spark-data-analyzer
```

2. Compile o projeto:
```bash
mvn clean package
```

## ▶️ Execução

### Usando Maven

```bash
# Para Java 8/11
mvn exec:java -Dexec.mainClass="com.dataanalyzer.DataAnalyzer"

# Para Java 17+
mvn exec:java -Dexec.mainClass="com.dataanalyzer.DataAnalyzer" -Dexec.args="" \
-Dexec.cleanupDaemonThreads=false \
-Dexec.jvmArgs="--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
```

### Usando o JAR compilado

```bash
# Para Java 8/11
java -jar target/java-spark-data-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar

# Para Java 17+
java --add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
-jar target/java-spark-data-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## ❓ Solução de Problemas

### Windows e Hadoop

O Spark usa algumas funcionalidades do Hadoop que podem gerar avisos no Windows. Se encontrar avisos relacionados ao `winutils.exe` ou `HADOOP_HOME`, você pode:

1. Ignorá-los (não afetam a funcionalidade básica do aplicativo)
2. Configurar o ambiente Hadoop para Windows:
    - Baixe o [winutils.exe](https://github.com/cdarlint/winutils)
    - Crie uma pasta `C:\hadoop\bin` e coloque o arquivo lá
    - Configure a variável de ambiente `HADOOP_HOME=C:\hadoop`
    - Adicione `%HADOOP_HOME%\bin` ao PATH

### Aviso de "illegal reflective access"

Este aviso pode aparecer ao usar Java 11. É normal e não afeta o funcionamento do aplicativo.

## 📊 Dados de Exemplo

O projeto inclui um arquivo de exemplo `dados_vendas.csv` com dados fictícios de vendas de produtos eletrônicos para testar as funcionalidades do aplicativo. Este arquivo contém os seguintes campos:

| Campo | Descrição |
|-------|-----------|
| ID | Identificador único da venda |
| Data | Data da venda (formato YYYY-MM-DD) |
| Produto | Nome do produto vendido |
| Categoria | Categoria do produto |
| Preco | Preço unitário do produto |
| Quantidade | Quantidade vendida |
| ClienteID | Identificador do cliente |
| Regiao | Região geográfica da venda |
| Vendedor | Nome do vendedor |
| Desconto | Percentual de desconto aplicado (decimal) |

## 📝 Uso Básico

Ao iniciar o aplicativo, você verá um menu interativo:

1. **Carregue os dados** usando a opção 1
    - Digite o caminho para o CSV ou use "example" para o arquivo de exemplo
    - Confirme se o arquivo tem cabeçalho (s/n)
    - Especifique o delimitador (geralmente vírgula)

2. **Explore os dados**:
    - Opção 2: Ver a estrutura (schema) dos dados
    - Opção 3: Ver uma amostra dos dados
    - Opção 4: Ver estatísticas descritivas

3. **Analise e transforme os dados**:
    - Opção 5: Filtrar registros
    - Opção 6: Agregar dados (ex: soma de vendas por região)
    - Opção 7: Transformar dados (criar colunas, renomear, etc.)

4. **Salve os resultados** usando a opção 8

## 🤝 Contribuições

Contribuições são bem-vindas! Se você encontrar bugs ou tiver sugestões de melhorias, abra uma issue ou envie um pull request.

## 📜 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo LICENSE para detalhes.

---

<div align="center">
  <p><strong>Por que Java para análise de dados?</strong> Desempenho superior, tipagem estática, multithreading robusto e integração perfeita com sistemas empresariais.</p>
  <p><em>Java Spark Data Analyzer - A resposta Java para a análise de dados em Python.</em></p>
</div>