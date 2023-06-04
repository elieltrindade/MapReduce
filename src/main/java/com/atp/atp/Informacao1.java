/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atp.atp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Informacao1 {
  // Declaração dos parâmetros da entrada (chave e formato) e formato da chave que irá gerar e do valor
    public static class Mapperinformacao1 extends Mapper<Object, Text, Text, IntWritable>{
        // Implementação da classe, cria função, o contexto que auxilia na transformação da chave-valor
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); // Converte os valores para String (pois estão em tipo Text)
            String[] campos = linha.split(";"); // Reconhece o delimitador do arquivo CSV
            if(campos.length == 10){ // Verifica se a linha não está quebrada
                String pais = campos[0]; // Acessa o valor (nome dos países)
                int transacoes = 1; // Determina a ocorrência
                // Converte chave-valor
                Text chaveMap = new Text(pais); // Converte String para Text (Hadoop)
                IntWritable valorMap = new IntWritable(transacoes); // Converte int para IntWritable (Hadoop)

                context.write(chaveMap, valorMap); // Escreve chave e valor
            }    
        }   
    }

    // Declaração dos parâmetros da entrada (chave) e o tipo do valor que recebe do Map e formato de saída
    public static class Reducerinformacao1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        // Parâmetros que o Reduce recebe
        // Iterable pois recebe uma lista de valores
        Text paisMaiorTransacoes;
        int maiorTransacoes;
        
        @Override
        public void setup(Context context) {
            paisMaiorTransacoes = new Text();
            maiorTransacoes = Integer.MIN_VALUE;
        }
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            // Agregar a partir da chave e dos valores
            int soma = 0;
            for(IntWritable val : valores){
                soma += val.get(); // Recebe um número inteiro e converte para um tipo IntWritable
            }
            if (soma > maiorTransacoes) {
               maiorTransacoes = soma;
               paisMaiorTransacoes.set(chave);
            }
            
            //System.out.println(chave + " - " + soma);
            //passa todos os valores para o arquivo de saida
            //IntWritable valorSaida = new IntWritable(soma); // Converte para o tipo IntWritable (Hadoop)
            //context.write(chave, valorSaida); // Escreve chave e valor
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(paisMaiorTransacoes, new IntWritable(maiorTransacoes)); //passa pais com maior transicoes
            System.out.println("País com maior número de transações: " + paisMaiorTransacoes.toString());
            System.out.println("Quantidade de transações: " + maiorTransacoes);
        }
    }    
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/martins.eliel/Desktop/atp/informacao1";
        
        // Se o tamanho da quantidade de parâmetros que o programa recebe for maior que 2, 
        // significa que alguém está chamando o programa externamente.
        // Isso permite que o código rode tanto localmente quanto no HDFS.
        if(args.length == 2){
            arquivoEntrada = args[0]; // Caminho de entrada no HDFS
            arquivoSaida = args[1];  // Caminho de saída no HDFS
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade1");
        
        job.setJarByClass(Informacao1.class); // Define o JAR
        job.setMapperClass(Mapperinformacao1.class); // Define o Mapper
        job.setReducerClass(Reducerinformacao1.class); // Define o Reducer      
        job.setOutputKeyClass(Text.class); // Define o tipo da chave gerada
        job.setOutputValueClass(IntWritable.class); // Define o tipo do valor gerado
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); // Informa o arquivo de entrada
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); // Informa o arquivo de saída
        
        // Submete a tarefa no MapReduce
        if (job.waitForCompletion(true)) {
            System.exit(0); // Finaliza o programa após o job ser concluído
        } else {
            System.exit(1); // Caso ocorra algum erro na execução do job
        }
    }
} 