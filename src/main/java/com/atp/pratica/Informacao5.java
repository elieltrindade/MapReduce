/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//Mercadoria com maior quantidade de transações financeiras em 2016.
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
public class Informacao5 {
  
    public static class Mapperinformacao5 extends Mapper<Object, Text, Text, IntWritable>{
       
        @Override
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
               mercadoriaMaiorTransacoes2016.set(chave + ";"+soma);
            }            
        }
                @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            String legenda = "Mercadoria com maior quantidade de transações financeiras em 2016 \nMercadoria;Transacoes";
            context.write(new Text(legenda), null);
            context.write(mercadoriaMaiorTransacoes2016, null);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "caminho/do/arquivo/entrada.csv";
        String arquivoSaida = "caminho/do/arquivo/saida.csv";     
        
       
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
