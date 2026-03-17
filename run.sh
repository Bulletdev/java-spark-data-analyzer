#!/bin/bash
set -e

CLASSES_DIR="target/classes"

# Recompila só se não houver classes ou houver fontes mais novos que elas
if [ ! -d "$CLASSES_DIR" ] || find src/main/java -name "*.java" -newer "$CLASSES_DIR" | grep -q .; then
  echo "Compilando..."
  mvn compile -q
else
  echo "Fontes sem alteração, pulando compilação."
fi

# Monta classpath com todas as dependências do projeto
CP=$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)

java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -cp "target/classes:$CP" \
  com.dataanalyzer.DataAnalyzer
