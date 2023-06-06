/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//Mercadoria com maior total de peso, de acordo com todas as transações comerciais, separadas por ano.
package com.atp.pratica;

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

/**
 *
 * @author martins.eliel
 */
public class Informacao8 {
  
    public static class Mapperinformacao4 extends Mapper<Object, Text, Text, IntWritable>{
       
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); 
            String[] campos = linha. split(";"); 
            if(campos.length == 10 ){ 
                String mercadoria = campos[3];
                int transacoes = 1; 
                
                Text chaveMap = new Text(mercadoria); 
                IntWritable valorMap = new IntWritable(transacoes);

                context.write(chaveMap, valorMap); 
                
            }    
        }   
    }
    
    public static class Reducerinformacao4 extends Reducer<Text, IntWritable, Text,IntWritable>{
        
        Text mercadoriaMaiorTransacoes;
        int maiorTransacoes;
        
        @Override
        public void setup(Context context) {
            mercadoriaMaiorTransacoes = new Text();
            maiorTransacoes = Integer.MIN_VALUE;
        }
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
          
            int soma = 0;
            for(IntWritable val : valores){
                soma += val.get(); 
            }
            if (soma > maiorTransacoes) {
               maiorTransacoes = soma;
               mercadoriaMaiorTransacoes.set(chave);
            }
            System.out.println(chave+ " - "+ soma );
            //IntWritable valorSaida = new IntWritable(soma); 
           // context.write(chave, valorSaida); 
        }
                @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(mercadoriaMaiorTransacoes, new IntWritable(maiorTransacoes)); //passa pais com maior transicoes
            System.out.println("Mercadoria com maior número de transações: " + mercadoriaMaiorTransacoes.toString());
            System.out.println("Quantidade de transações: " + maiorTransacoes);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/martins.eliel/Desktop/atp/informacao4";
        
       
        if(args.length == 2){
            arquivoEntrada = args[0]; 
            arquivoSaida = args[1];  
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade4");
        
        job.setJarByClass(Informacao4.class); 
        job.setMapperClass(Mapperinformacao4.class); 
        job.setReducerClass(Reducerinformacao4.class);        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); 
        
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); 
        
        job.waitForCompletion(true);
                 
    }   
}