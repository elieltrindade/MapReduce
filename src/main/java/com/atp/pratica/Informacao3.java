/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//Quantidade de transações comerciais realizadas por ano.
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
public class Informacao3 {
  
    public static class Mapperinformacao3 extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); 
            String[] campos = linha. split(";"); 
            if(campos.length == 10 ){ 
                String ano = campos[1]; 
                int transacoes = 1; 
                
                Text chaveMap = new Text(ano); 
                IntWritable valorMap = new IntWritable(transacoes);
                context.write(chaveMap, valorMap);                 
            }    
        }   
    }
    
    public static class Reducerinformacao3 extends Reducer<Text, IntWritable, Text,IntWritable>{
        
        Text ano;
        
        @Override
        public void setup(Context context) {
            ano = new Text();            
        }
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
                    
            int soma = 0;
            for(IntWritable val : valores){
                soma += val.get();
            }
            ano.set(ano.toString() + chave + ";" + soma + "\n"); 
        }
        
    
        @Override
            public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            String legendAno = "Quantidade de Transacoes por ano: \nAno;Transacoes";
               context.write(new Text(legendAno), null);
               context.write(ano, null);     
            }                                          
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/martins.eliel/Desktop/atp/informacao3";
        
    
        if(args.length == 2){
            arquivoEntrada = args[0]; 
            arquivoSaida = args[1]; 
        }    
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade3");
        
        job.setJarByClass(Informacao3.class); 
        job.setMapperClass(Mapperinformacao3.class); 
        job.setReducerClass(Reducerinformacao3.class);        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); 
        
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); 
   
        job.waitForCompletion(true);
                 
    }   
}