# README

Este repositório contém o código e o arquivo de configuração necessários para executar um programa MapReduce que processa dados de transações comerciais. A seguir, você encontrará as ferramentas necessárias para executar o código, uma explicação do arquivo **pom.xml** e uma descrição detalhada do código em si.

## Ferramentas necessárias

Para executar o código, você precisará das seguintes ferramentas:

* Java Development Kit (JDK) versão 18 ou superior
* Apache Maven
* Hadoop versão 2.7.3
  
Certifique-se de ter instalado corretamente todas as dependências antes de executar o programa.

## Arquivo pom.xml
O arquivo pom.xml é um arquivo de configuração usado pelo Apache Maven para gerenciar o projeto Java e suas dependências. Ele contém informações sobre o projeto, como as dependências necessárias, o nome do projeto, a versão e outros detalhes.

Aqui está uma breve explicação das principais seções do arquivo pom.xml:

* '<groupId>', '<artifactId>', '<version>': Essas tags identificam exclusivamente o projeto no repositório Maven.
'<packaging>': Especifica o tipo de artefato que será construído (no caso, um arquivo JAR).
'<properties>': Define as propriedades do projeto, como codificação de origem e versões do compilador Java.
'<dependencies>': Lista as dependências do projeto, que são bibliotecas externas necessárias para a execução do código.
'<name>': Nome do projeto.

Certifique-se de que as dependências mencionadas no arquivo **pom.xml** estão corretamente instaladas e acessíveis antes de executar o código.

## Descrição do código

O código em questão implementa um programa MapReduce para processar dados de transações comerciais. Ele consiste em duas classes principais: Mapperinformacao1 e Reducerinformacao1.

A classe **'Mapperinformacao1'** é responsável pela função de mapeamento no processo MapReduce. Ela recebe um conjunto de pares chave-valor de entrada, processa os dados e emite novos pares chave-valor intermediários. Nesse caso, o mapper lê linhas de um arquivo CSV, extrai o nome do país de cada linha e emite um par chave-valor, onde a chave é o nome do país e o valor é o número de transações (inicializado como 1).

A classe **'Reducerinformacao1'** é responsável pela função de redução no processo MapReduce. Ela recebe os pares chave-valor intermediários gerados pelo mapeador, realiza uma operação de agregação e emite os pares chave-valor de saída final. Nesse caso, o reducer soma todos os valores recebidos para cada chave e encontra o país com o maior número de transações. No método **'cleanup()'**, ele emite o país com o maior número de transações como resultado final.

O método **'main()'** é o ponto de entrada do programa. Ele configura e executa o trabalho MapReduce, especificando o caminho de entrada e saída, as classes do mapeador e do reducer, bem como os tipos de chave-valor esperados.
O código também é compatível com a execução no Hadoop, pois possui suporte para passar os caminhos de entrada e saída do HDFS como argumentos.

Certifique-se de fornecer corretamente os caminhos de entrada e saída no código antes de executá-lo.

Espero que estas informações sejam úteis.
