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

/**
 *
 * @author martins.eliel
 */
public class Informacao5 {
  
    public static class Mapperinformacao5 extends Mapper<Object, Text, Text, IntWritable>{
       
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); 
            String[] campos = linha. split(";"); 
            if(campos.length == 10 && "2016".equals(campos[1])){ 
                String mercadoria = campos[3];
                int transacoes = 1; 
                
                Text chaveMap = new Text(mercadoria); 
                IntWritable valorMap = new IntWritable(transacoes);

                context.write(chaveMap, valorMap); 
                
            }    
        }   
    }
    
    public static class Reducerinformacao5 extends Reducer<Text, IntWritable, Text,IntWritable>{
        
        Text mercadoriaMaiorTransacoes2016;
        int maiorTransacoes;
        
        @Override
        public void setup(Context context) {
            mercadoriaMaiorTransacoes2016 = new Text();
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
               mercadoriaMaiorTransacoes2016.set(chave);
            }
            System.out.println(chave+ " - "+ soma );
            //IntWritable valorSaida = new IntWritable(soma); 
           // context.write(chave, valorSaida); 
        }
                @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(mercadoriaMaiorTransacoes2016, new IntWritable(maiorTransacoes)); //passa pais com maior transicoes
            System.out.println("Mercadoria com maior número de transações: " + mercadoriaMaiorTransacoes2016.toString());
            System.out.println("Quantidade de transações: " + maiorTransacoes);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/martins.eliel/Desktop/atp/informacao5";
        
       
        if(args.length == 2){
            arquivoEntrada = args[0]; 
            arquivoSaida = args[1];  
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade5");
        
        job.setJarByClass(Informacao5.class); 
        job.setMapperClass(Mapperinformacao5.class); 
        job.setReducerClass(Reducerinformacao5.class);        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); 
        
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); 
        
        job.waitForCompletion(true);
                 
    }   
}
