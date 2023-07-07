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
        
    public static class Mapperinformacao8 extends Mapper<Object, Text, Text, Text>{
       
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); 
            String[] campos = linha. split(";"); 
            if(campos.length == 10 ){ 
                String ano = campos[1];
                try{ //Tratamento caso peso for String
                    int peso = Integer.parseInt(campos[6]); // transforma uma String em inteiro
                    String mercadoria = campos[3];
                    Text chaveMap = new Text(ano); 
                    Text valorMap = new Text(mercadoria+";"+peso);
                    context.write(chaveMap, valorMap);
                }
                catch(NumberFormatException e){                                 
                }               
            }    
        }   
    }
    
    public static class Reducerinformacao8 extends Reducer<Text, Text, Text,Text>{                     
        
        boolean legenda;
        
        @Override
        public void setup (Context context){
            legenda = false;
        }
        
        @Override
        public void reduce(Text chave, Iterable<Text> valores, Context context) throws IOException, InterruptedException{                             
            int maiorPeso = Integer.MIN_VALUE;            
            String mercadoriaMaiorPeso = "";
            String ano = "";
            
            for(Text val : valores){
                String[] campos = val.toString() .split(";");
                String mercadoria = campos[0];                
                int peso = Integer.parseInt(campos[1]);

                if (peso >= maiorPeso) {  //Deve ficar dentro da estrutura de repeticao para achar o maior peso dentro da mesma chave
                   maiorPeso = peso;        //se nao ele compara apenas o primeiro valor de cada chave                   
                   mercadoriaMaiorPeso = mercadoria;
                   ano = chave.toString();
                }
            }
            if (legenda == false){
                String texto = "Mercadoria com maior peso por ano\nAno;Mercadoria;Maior Peso";
                context.write(new Text(texto),null);
                legenda = true;
            }
            context.write(null, new Text(new Text(ano)+";"+mercadoriaMaiorPeso + ";"+ maiorPeso));
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
        Job job = Job.getInstance(conf, "atividade81");
        
        job.setJarByClass(Informacao8.class); 
        job.setMapperClass(Mapperinformacao8.class); 
        job.setReducerClass(Reducerinformacao8.class);        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class); 
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); 
        
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); 
        
        job.waitForCompletion(true);
                 
    }   
}