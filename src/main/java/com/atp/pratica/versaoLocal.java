/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//País com a maior quantidade de transações comerciais efetuadas.
package com.atp.pratica;

import java.io.BufferedReader; 
//Fornece classes para ler texto de um fluxo de entrada de caracteres,como um arquivo.
//Neste código, é usado para ler o arquivo de entrada linha por linha.
import java.io.FileReader;
//Fornece uma maneira conveniente de ler dados de um arquivo de caracteres. 
//É usado em conjunto com BufferedReader para ler o arquivo de entrada.
import java.io.IOException;
//É uma exceção que pode ser lançada quando ocorre um erro de I/O (entrada/saída).
//É usada para capturar e lidar com exceções relacionadas à leitura do arquivo de entrada.
import java.util.HashMap;
//Implementa a interface Map e fornece uma implementação de mapa baseada em tabela hash.
//Neste código, é usado para armazenar o número de transações por país.
import java.util.Map;
//É uma interface que representa uma estrutura de dados de mapeamento chave-valor.
//Neste código, é usado como o tipo de dados do mapa de transações.

public class versaoLocal {

    public static void main(String[] args) {
        String arquivoEntrada = "caminho/do/arquivo/entrada.csv";

        // Se o tamanho da quantidade de argumentos passados for maior que 0,
        // assume que o arquivo de entrada foi fornecido como argumento
        if (args.length > 0) {
            arquivoEntrada = args[0];
        }

        // Mapa para armazenar o número de transações por país
        Map<String, Integer> mapaTransacoes = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(arquivoEntrada))) {
            String linha;
            while ((linha = br.readLine()) != null) {
                String[] campos = linha.split(";");

                // Verifica se a linha não está quebrada
                if (campos.length == 10) {
                    String pais = campos[0];
                    // Incrementa o número de transações do país no mapa
                    mapaTransacoes.put(pais, mapaTransacoes.getOrDefault(pais, 0) + 1);
                }
            }
        } catch (IOException e) {
            System.out.println("Erro ao ler o arquivo de entrada: " + e.getMessage());
            System.exit(1);
        }

        // Encontra o país com o maior número de transações
        String paisMaiorTransacoes = "";
        int maiorTransacoes = 0;
        for (Map.Entry<String, Integer> entry : mapaTransacoes.entrySet()) {
            if (entry.getValue() > maiorTransacoes) {
                maiorTransacoes = entry.getValue();
                paisMaiorTransacoes = entry.getKey();
            }
        }

        // Imprime o resultado
        System.out.println("País com maior número de transações: " + paisMaiorTransacoes);
        System.out.println("Número de transações: " + maiorTransacoes);
    }
}