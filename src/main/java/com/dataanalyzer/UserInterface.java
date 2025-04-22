package com.dataanalyzer;

import java.util.Scanner;

public class UserInterface {

    private SparkOperations sparkOperations;
    private Scanner scanner;

    public UserInterface(SparkOperations sparkOperations) {
        this.sparkOperations = sparkOperations;
        this.scanner = new Scanner(System.in); // Scanner gerenciado internamente
    }

    public void displayMenu() {
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
    }

    public void run() { // Renomeado de runInteractive para run
        boolean running = true;
        System.out.println("\n=== Java Data Analyzer ===");

        while (running) {
            displayMenu();
            int choice = -1;
            try {
                String input = scanner.nextLine();
                choice = Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.println("Entrada inválida. Por favor, digite um número.");
                continue; // Pula para a próxima iteração do loop
            } catch (Exception e) { // Captura outras possíveis exceções de leitura
                 System.out.println("Erro ao ler entrada: " + e.getMessage());
                 continue;
            }

            switch (choice) {
                case 1:
                    sparkOperations.loadData(scanner); // Passa o scanner para métodos que precisam dele
                    break;
                case 2:
                    sparkOperations.showSchema();
                    break;
                case 3:
                    sparkOperations.showSample();
                    break;
                case 4:
                    sparkOperations.showStats();
                    break;
                case 5:
                    sparkOperations.filterData(scanner);
                    break;
                case 6:
                    sparkOperations.aggregateData(scanner);
                    break;
                case 7:
                    sparkOperations.transformData(scanner);
                    break;
                case 8:
                    sparkOperations.saveResults(scanner);
                    break;
                case 9:
                    running = false;
                    System.out.println("Encerrando aplicação...");
                    break;
                default:
                    System.out.println("Opção inválida. Tente novamente.");
            }
        }
        // O Scanner não é fechado aqui, pois System.in não deve ser fechado
        // scanner.close();
    }
} 