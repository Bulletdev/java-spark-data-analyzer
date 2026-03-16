#!/bin/bash
set -e

JAR="target/java-spark-data-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar"

# Recompila só se o JAR não existe ou há fontes mais novos que ele
if [ ! -f "$JAR" ] || find src -name "*.java" -newer "$JAR" | grep -q .; then
  echo "Compilando..."
  mvn package -DskipTests -q
else
  echo "Fontes sem alteração, pulando compilação."
fi

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
  -jar "$JAR"
