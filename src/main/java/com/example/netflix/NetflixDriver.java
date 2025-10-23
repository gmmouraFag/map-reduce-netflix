package com.example.netflix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public class NetflixDriver {
    public static void main(String[] args) throws Exception {
        Instant start = Instant.now();
        if (args.length < 2) {
            System.err.println("Uso: hadoop jar netflix-mapreduce.jar <input> <output> [--ignore-sw]");
            System.exit(1);
        }

        // LOG: argumentos recebidos
        System.out.println("=== DRIVER: Iniciando job Netflix Descriptions Analysis ===");
        System.out.println("Args recebidos (" + args.length + "): " + Arrays.toString(args));

        // Suporta dois formatos:
        // a) hadoop jar ... com.example.netflix.NetflixDriver <input> <output>
        // b) hadoop jar ... <input> <output>
        int base = 0;
        if (args[0].contains(".") && args[0].contains("com.example.netflix")) {
            base = 1;
            if (args.length - base < 2) {
                System.err.println("Uso (com classe): hadoop jar <jar> com.example.netflix.NetflixDriver <input> <output> [--ignore-sw]");
                System.exit(1);
            }
        }

        String inputPath = args[base];
        String outputPath = args[base + 1];

        boolean ignoreSW = false;
        if (args.length > base + 2 && "--ignore-sw".equalsIgnoreCase(args[base + 2])) {
            ignoreSW = true;
        }

        System.out.println("InputPath ..........: " + inputPath);
        System.out.println("OutputPath .........: " + outputPath);
        System.out.println("Ignore Stopwords ...: " + ignoreSW);

        Configuration conf = new Configuration();
        if (ignoreSW) conf.set("ignore.stopwords", "true");

        // Habilita/ajusta verbosidade de logs no Mapper/Reducer (padrão: false)
        // Rode com: -Dlog.verbose=true para ver mais detalhes por linha/chave
        if (System.getProperty("log.verbose") != null) {
            conf.set("log.verbose", System.getProperty("log.verbose"));
        }

        // Opcional: tamanho mínimo do token, ex.: -Dmin.token.len=2
        if (System.getProperty("min.token.len") != null) {
            conf.set("min.token.len", System.getProperty("min.token.len"));
        }

        Job job = Job.getInstance(conf, "Netflix Descriptions Analysis (Simple)");
        job.setJarByClass(NetflixDriver.class);

        job.setMapperClass(NetflixMapper.class);
        job.setReducerClass(NetflixReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.out.println("=== DRIVER: Submetendo job ao YARN/MapReduce ===");
        boolean ok = job.waitForCompletion(true);

        System.out.println("=== DRIVER: Job finalizado. Sucesso=" + ok + " ===");
        Instant end = Instant.now();
        System.out.println("Tempo total (s) ....: " + Duration.between(start, end).getSeconds());


        System.exit(ok ? 0 : 1);
    }
}
