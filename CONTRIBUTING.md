# Contributing

## Configurando o ambiente

```bash
git clone <repo-url>
cd java-spark-data-analyzer
mvn compile
```

Requer Java 11+ e Maven 3.8+.

## Rodando os testes antes de submeter

```bash
mvn verify
```

Todos os testes devem passar. A cobertura de testes do pacote `core` deve
permanecer acima de 70%.

## Estrutura de pacotes

| Pacote       | Responsabilidade                        |
|--------------|-----------------------------------------|
| `core`       | Lógica de dados — stateless, testável   |
| `ui`         | Apresentação CLI                        |
| `config`     | Configuração externalizada              |
| `session`    | Ciclo de vida do Spark                  |
| `util`       | Utilitários reutilizáveis               |

## Convenções

- Novos métodos em `DataTransformer` e `DataAggregator` devem ser **stateless**
  (recebem e retornam `Dataset<Row>`)
- Toda classe e método público deve ter **JavaDoc**
- Erros devem lançar `IllegalArgumentException` com mensagem descritiva —
  nunca silenciosos
- Sem `Thread.sleep()` em operações de dado
- Sem `System.out.println` em classes do pacote `core` — use SLF4J

## Convenção de commits

```
tipo: descrição curta em inglês

Exemplos:
feat: add JSON schema validation
fix: handle null groupBy column in DataAggregator
refactor: extract SortSpec to dedicated class
test: add edge cases for DataTransformer.filter
```
