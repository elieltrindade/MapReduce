/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/*Mercadoria com maior quantidade de transações financeiras em 2016, no Brasil 
(como a base de dados está em inglês, utilize Brazil).*/

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
public class Informacao6 {
  
    public static class Mapperinformacao6 extends Mapper<Object, Text, Text, IntWritable>{
       
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString(); 
            String[] campos = linha. split(";"); 
            if(campos.length == 10 && "2016".equals(campos[1]) && "Brazil".equals(campos[0])){ 
                String mercadoria = campos[3];
                int transacoes = 1; 
                
                Text chaveMap = new Text(mercadoria); 
                IntWritable valorMap = new IntWritable(transacoes);

                context.write(chaveMap, valorMap); 
                
            }    
        }   
    }
    
    public static class Reducerinformacao6 extends Reducer<Text, IntWritable, Text,IntWritable>{
        
        Text mercadoriaMaiorTransacoes2016Brazil;
        int maiorTransacoes;
               
        @Override
        public void setup(Context context) {
            mercadoriaMaiorTransacoes2016Brazil = new Text();
            maiorTransacoes = Integer.MIN_VALUE;
        }
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
          
            int soma = 0;
            for(IntWritable val : valores){
                soma += val.get(); 
            }
             if (soma >= maiorTransacoes) {
                if (soma > maiorTransacoes) {
                    maiorTransacoes = soma;
                    mercadoriaMaiorTransacoes2016Brazil.set(chave);
                } else { // Se o valor for igual ao maior, também adiciona ao arquivo                    
                    mercadoriaMaiorTransacoes2016Brazil.set(mercadoriaMaiorTransacoes2016Brazil.toString() + 
                            "\n"+ chave);                   
                }
            }                                                           
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            String legendaMercadoria = "Mercadoria com maior numero de transacoes no ano de 2016 no Brasil: \n" ;
            String legendaTransacoes = "\nQuantidade de Transacoes: \n";
               context.write(new Text(legendaMercadoria), null);
               context.write(mercadoriaMaiorTransacoes2016Brazil, null);
               context.write(new Text(legendaTransacoes), new IntWritable(maiorTransacoes));       
            }                                  
        }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/martins.eliel/Desktop/atp/informacao6";
        
       
        if(args.length == 2){
            arquivoEntrada = args[0]; 
            arquivoSaida = args[1];  
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade6");
        
        job.setJarByClass(Informacao6.class); 
        job.setMapperClass(Mapperinformacao6.class); 
        job.setReducerClass(Reducerinformacao6.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada)); 
        
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida)); 
        
        job.waitForCompletion(true);
                 
    }   
}
