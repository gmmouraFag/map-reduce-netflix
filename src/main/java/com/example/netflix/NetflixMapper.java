package com.example.netflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;

public class NetflixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD_KEY = new Text();
    private static final Text TOTAL_WORDS_KEY = new Text("TOTAL_WORDS");
    private static final Text DESCRIPTIONS_COUNT_KEY = new Text("DESCRIPTIONS_COUNT");

    // CSV: divide por vírgula respeitando aspas
    private static final Pattern CSV_SPLIT = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

    private boolean ignoreStopwords = false;
    private Set<String> stop = new HashSet<>();
    private int minTokenLen = 1;

    // Log control
    private boolean LOG_VERBOSE = false;
    private long lineCount = 0;
    private long loggedLines = 0;

    @Override
    protected void setup(Context ctx) {
        // liga/desliga stopwords
        ignoreStopwords = "true".equalsIgnoreCase(ctx.getConfiguration().get("ignore.stopwords", "false"));

        // tamanho mínimo do token (opcional): -Dmin.token.len=2
        try {
            minTokenLen = Integer.parseInt(ctx.getConfiguration().get("min.token.len", "1"));
            if (minTokenLen < 1) minTokenLen = 1;
        } catch (Exception ignored) { minTokenLen = 1; }

        // verbosidade: -Dlog.verbose=true
        LOG_VERBOSE = "true".equalsIgnoreCase(ctx.getConfiguration().get("log.verbose", "false"));

        if (ignoreStopwords) {
            // PT-BR + EN (básico)
            String[] arr = ("a,à,á,â,ã,ao,aos,as,com,como,da,das,de,dele,dela,delas,deles,do,dos,e,é,em,entre,essa,esse,esta,este," +
                    "eu,ela,ele,elas,eles,isso,isto,la,lo,na,nas,no,nos,né,num,nums,numa,umas,uns,ou,os,o,para,pra,pras,pelos,pelas,pelo,pela," +
                    "por,porque,que,se,sem,sua,suas,seu,seus,tem,ter,um,uma,você,voces,vcs," +
                    "the,a,an,and,or,of,for,to,in,on,with,as,by,is,are,was,were,be,been,being,at,from,this,that,these,those,it,its,into,about,over,under," +
                    "after,before,between,while,than,so,not,no,do,does,did,done,can,could,should,would")
                    .toLowerCase(Locale.ROOT).split("\\s*,\\s*");
            for (String s : arr) stop.add(stripAccents(s));
        }

        System.out.println("=== MAPPER: setup ===");
        System.out.println("Ignore Stopwords ....: " + ignoreStopwords);
        System.out.println("Min Token Len .......: " + minTokenLen);
        System.out.println("Log Verbose .........: " + LOG_VERBOSE);
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) {
        lineCount++;
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // header (conferência simples)
        if (key.get() == 0 && line.toLowerCase(Locale.ROOT).contains("title") && line.toLowerCase(Locale.ROOT).contains("description")) {
            if (LOG_VERBOSE) {
                System.out.println("MAPPER: detectado header na primeira linha, ignorando.");
            }
            return;
        }

        String[] cols = CSV_SPLIT.split(line, -1);
        if (cols.length < 12) {
            if (LOG_VERBOSE) {
                System.out.println("MAPPER: linha ignorada por ter menos de 12 colunas. Linha=" + line);
            }
            return;
        }

        String title = unquote(cols[2]);
        String description = unquote(cols[11]);

        // normalização robusta
        String norm = normalize(description);
        // sempre contamos a descrição para média (mesmo vazia)
        writeQuiet(ctx, DESCRIPTIONS_COUNT_KEY, ONE);

        if (norm.isEmpty()) {
            Text lenKey = new Text("TITLELEN:" + title);
            writeQuiet(ctx, lenKey, new IntWritable(0));

            // Log amostral (primeiras 5 linhas vazias)
            if (loggedLines < 5) {
                System.out.println("MAPPER: descrição vazia para título: \"" + title + "\" (len=0).");
                loggedLines++;
            }
            return;
        }

        String[] tokens = norm.split("\\s+");
        int accepted = 0;

        for (String t : tokens) {
            if (t.isEmpty()) continue;
            if (t.length() < minTokenLen) continue;

            String tok = stripAccents(t);
            if (ignoreStopwords && stop.contains(tok)) continue;

            WORD_KEY.set("WORD:" + tok);
            writeQuiet(ctx, WORD_KEY, ONE);
            accepted++;
        }

        // totais e estatísticas por descrição
        writeQuiet(ctx, TOTAL_WORDS_KEY, new IntWritable(accepted));
        Text lenKey = new Text("TITLELEN:" + title);
        writeQuiet(ctx, lenKey, new IntWritable(accepted));

        // Log amostral: primeiras 5 linhas e depois a cada 100k
        if (loggedLines < 5 || (lineCount % 100000 == 0)) {
            System.out.println("MAPPER: título=\"" + title + "\" | tokens aceitos=" + accepted + " | exemploTexto=\"" +
                    (norm.length() > 60 ? norm.substring(0, 60) + "..." : norm) + "\"");
            loggedLines++;
        }
    }

    private static String unquote(String s) {
        if (s == null) return "";
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static String normalize(String s) {
        if (s == null) return "";
        String lower = s.toLowerCase(Locale.ROOT);
        String noAccents = stripAccents(lower);
        // mantém letras (qualquer idioma), dígitos e espaço
        noAccents = noAccents.replaceAll("[^\\p{L}\\p{Nd}'\\s]+", " ");
        return noAccents.trim().replaceAll("\\s{2,}", " ");
    }

    private static String stripAccents(String input) {
        String n = Normalizer.normalize(input, Normalizer.Form.NFD);
        return n.replaceAll("\\p{M}+", "");
    }

    private static void writeQuiet(Context ctx, Text k, IntWritable v) {
        try { ctx.write(k, v); } catch (Exception ignored) {}
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("=== MAPPER: cleanup ===");
        System.out.println("Total de linhas lidas pelo map task: " + lineCount);
    }
}
