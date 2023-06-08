/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//Mercadoria com maior total de peso, de acordo com todas as transações comerciais.
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
public class Informacao7 {
  
    public static class Mapperinformacao7 extends Mapper<Object, Text, Text, IntWritable>{
       
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); 
            String[] campos = linha. split(";"); 
            if(campos.length == 10 ){ 
                String mercadoria = campos[3];
                try{ //Tratamento caso peso for String
                    int peso = Integer.parseInt(campos[6]); // transforma uma String em inteiro
                
                    Text chaveMap = new Text(mercadoria); 
                    IntWritable valorMap = new IntWritable(peso);

                    context.write(chaveMap, valorMap);
                }
                catch(NumberFormatException e){                
                 
                }               
            }    
        }   
    }
    
    public static class Reducerinformacao7 extends Reducer<Text, IntWritable, Text,IntWritable>{
        
        Text mercadoriaMaiorTotalPeso;
        int maiorPeso;
        
        @Override
        public void setup(Context context) {
            mercadoriaMaiorTotalPeso = new Text();
            maiorPeso = Integer.MIN_VALUE;
        }
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
          
            int peso = 0;
            for(IntWritable val : valores){
                peso = val.get(); 
            
                if (peso > maiorPeso) {  //Deve ficar dentro da estrutura de repeticao para achar o maior peso dentro da mesma chave
                   maiorPeso = peso;        //se nao ele compara apenas o primeiro valor de cada chave
                   mercadoriaMaiorTotalPeso.set(chave+";"+peso);
                }
            }

        }
                @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            String legenda = "Mercadoria com maior peso\nMercadoria;Peso";
            context.write(new Text(legenda),null);
            context.write(mercadoriaMaiorTotalPeso, null);                    
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/martins.eliel/Desktop/atp/informacao7";
        
       
        if(args.length == 2){
            arquivoEntrada = args[0]; 
            arquivoSaida = args[1];  
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade7");
        
        job.setJarByClass(Informacao7.class); 
        job.setMapperClass(Mapperinformacao7.class); 
        job.setReducerClass(Reducerinformacao7.class);        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); 
        
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); 
        
        job.waitForCompletion(true);
                 
    }   
}