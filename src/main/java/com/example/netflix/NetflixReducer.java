package com.example.netflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class NetflixReducer extends Reducer<Text, IntWritable, Text, Text> {

    // contagens finais das palavras
    private final Map<String,Integer> wordCounts = new HashMap<>();

    // globais
    private long totalWords = 0L;
    private long descCount  = 0L;
    private String maxTitle = "";
    private int    maxLen   = Integer.MIN_VALUE;
    private String minTitle = "";
    private int    minLen   = Integer.MAX_VALUE;

    // Títulos e seus comprimentos (para decidir max/min)
    private final Map<String,Integer> titleLens = new HashMap<>();

    // Log control
    private boolean LOG_VERBOSE = false;
    private long keysProcessed = 0;

    @Override
    protected void setup(Context context) {
        LOG_VERBOSE = "true".equalsIgnoreCase(context.getConfiguration().get("log.verbose", "false"));
        System.out.println("=== REDUCER: setup ===");
        System.out.println("Log Verbose .........: " + LOG_VERBOSE);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
        keysProcessed++;
        String k = key.toString();

        if (k.startsWith("WORD:")) {
            String word = k.substring(5);
            int sum = 0; for (IntWritable v : values) sum += v.get();
            wordCounts.merge(word, sum, Integer::sum);

        } else if ("TOTAL_WORDS".equals(k)) {
            for (IntWritable v : values) totalWords += v.get();

        } else if ("DESCRIPTIONS_COUNT".equals(k)) {
            for (IntWritable v : values) descCount += v.get();

        } else if (k.startsWith("TITLELEN:")) {
            String title = k.substring("TITLELEN:".length());
            int sum = 0; for (IntWritable v : values) sum += v.get(); // normalmente só 1 valor
            titleLens.put(title, sum);
        }

        // Log amostral: primeiras 5 chaves e depois a cada 200k chaves
        if (keysProcessed <= 5 || (keysProcessed % 200000 == 0)) {
            System.out.println("REDUCER: processada chave #" + keysProcessed + " => " + k);
        }
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        System.out.println("=== REDUCER: agregando resultados finais ===");
        System.out.println("Títulos agregados ...: " + titleLens.size());
        System.out.println("Palavras distintas ..: " + wordCounts.size());
        System.out.println("TotalWords parcial ..: " + totalWords);
        System.out.println("DescCount parcial ...: " + descCount);

        // define max/min usando titleLens
        for (Map.Entry<String,Integer> e : titleLens.entrySet()) {
            int len = e.getValue();
            String title = e.getKey();
            if (len > maxLen) { maxLen = len; maxTitle = title; }
            if (len < minLen) { minLen = len; minTitle = title; }
        }

        // ----- TOP 5 (desc) -----
        List<Map.Entry<String,Integer>> descList = new ArrayList<>(wordCounts.entrySet());
        descList.sort((a,b) -> Integer.compare(b.getValue(), a.getValue()));
        List<Map.Entry<String,Integer>> top5 = new ArrayList<>(
                descList.subList(0, Math.min(5, descList.size()))
        );

        // ----- BOTTOM 5 (asc) -----
        List<Map.Entry<String,Integer>> ascList = new ArrayList<>(wordCounts.entrySet());
        ascList.sort(Comparator.comparingInt(Map.Entry::getValue));
        List<Map.Entry<String,Integer>> bottom5 = new ArrayList<>(
                ascList.subList(0, Math.min(5, ascList.size()))
        );

        double avg = descCount == 0 ? 0.0 : (double) totalWords / (double) descCount;

        // Escreve um resumo único (uma chave "SUMMARY" + várias linhas na "value")
        StringBuilder sb = new StringBuilder();
        sb.append("Normalizacao: minusculas + remocao de pontuacoes/caracteres especiais\n");
        sb.append(String.format("Titulo com descricao mais longa: %s (%d palavras)\n", nullSafe(maxTitle), Math.max(0, maxLen)));
        sb.append(String.format("Titulo com descricao mais curta: %s (%d palavras)\n", nullSafe(minTitle), Math.max(0, minLen)));

        sb.append("Top 5 palavras mais repetidas:\n");
        int idx = 1;
        for (Map.Entry<String,Integer> e : top5) {
            sb.append(String.format("  %d) %s\t%d\n", idx++, e.getKey(), e.getValue()));
        }

        sb.append("Top 5 palavras menos repetidas:\n");
        idx = 1;
        for (Map.Entry<String,Integer> e : bottom5) {
            sb.append(String.format("  %d) %s\t%d\n", idx++, e.getKey(), e.getValue()));
        }

        sb.append(String.format("Total de palavras (todas as descricoes): %d\n", totalWords));
        sb.append(String.format("Quantidade de descricoes: %d\n", descCount));
        sb.append(String.format("Media de palavras por descricao: %.4f\n", avg));

        // Logs finais no console
        System.out.println("=== REDUCER: resumo final ===");
        System.out.println("Maior descricao .....: \"" + nullSafe(maxTitle) + "\" (" + Math.max(0, maxLen) + " palavras)");
        System.out.println("Menor descricao .....: \"" + nullSafe(minTitle) + "\" (" + Math.max(0, minLen) + " palavras)");
        if (!top5.isEmpty()) {
            System.out.println("Top 3 palavras ......: " + top5.subList(0, Math.min(3, top5.size())));
        }

        ctx.write(new Text("SUMMARY"), new Text(sb.toString()));
        System.out.println("=== REDUCER: escrita do SUMMARY concluida ===");
    }

    private static String nullSafe(String s) {
        return (s == null || s.trim().isEmpty()) ? "UNKNOWN" : s;
    }
}
