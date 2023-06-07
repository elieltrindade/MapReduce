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
 *///Mercadoria com maior total de peso, de acordo com todas as transações comerciais, separadas por ano.
public class Informacao8 {
  
    public static class Mapperinformacao8 extends Mapper<Object, Text, Text, IntWritable>{        
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); 
            String[] campos = linha. split(";"); 
            if(campos.length == 10 ){ 
                String ano = campos[1];
                try{
                    int peso = Integer.parseInt(campos[6]);                 
                    Text chaveMap = new Text(ano); 
                    IntWritable valorMap = new IntWritable(peso);                
                    context.write(chaveMap, valorMap); 
                }
                catch(NumberFormatException e){
                            
                }             
            }    
        }   
    }
    
    public static class Reducerinformacao8 extends Reducer<Text, IntWritable, Text,IntWritable>{
        Text mercadoriaMaiorTotalPeso;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mercadoriaMaiorTotalPeso = new Text();
        } 
        
        public void reduce(Text chave, Iterable<IntWritable> valores, Reducer.Context context) throws IOException, InterruptedException{
        int maiorPeso = Integer.MIN_VALUE;
        String mercadoria = "";
    
        for (IntWritable val : valores) {
            int peso = val.get();

            if (peso > maiorPeso) {
                maiorPeso = peso;
                mercadoria = chave.toString();
            }
        }
    
        mercadoriaMaiorTotalPeso.set(mercadoria + ";" + maiorPeso);
}
                @Override
        public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            String legenda = """
                             Mercadoria com maior total de peso, de acordo com todas as transa\u00e7\u00f5es comerciais, separadas por ano
                             Ano - Mercadoria - Peso""";
            context.write(new Text(legenda),null);
            context.write(null , new Text(mercadoriaMaiorTotalPeso));
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/martins.eliel/Desktop/atp/informacao8";
        
       
        if(args.length == 2){
            arquivoEntrada = args[0]; 
            arquivoSaida = args[1];  
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade8");
        
        job.setJarByClass(Informacao8.class); 
        job.setMapperClass(Mapperinformacao8.class); 
        job.setReducerClass(Reducerinformacao8.class);        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class);
        
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); 
        
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); 
        
        job.waitForCompletion(true);
                 
    }   
}