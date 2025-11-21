package com.example.netflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NetflixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private final Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line == null || line.trim().isEmpty()) {
            return;
        }

        // pula o cabeçalho
        if (line.startsWith("show_id,")) {
            return;
        }

        List<String> fields = parseCsvLine(line);
        // Esperado: 12 colunas
        if (fields.size() < 12) {
            return;
        }

        String type        = safe(fields.get(1));  // Movie / TV Show
        String country     = safe(fields.get(5));
        String releaseYear = safe(fields.get(7));
        String rating      = safe(fields.get(8));
        String duration    = safe(fields.get(9));
        String listedIn    = safe(fields.get(10));

        // 1) Contagem por tipo
        if (!type.isEmpty()) {
            outKey.set("TYPE|" + type);
            context.write(outKey, ONE);
        }

        // 2) Contagem de gêneros globais e por ano
        if (!listedIn.isEmpty()) {
            String[] genres = listedIn.split(",\\s*");
            for (String g : genres) {
                if (g.isEmpty()) continue;

                // GENRE|Comedies
                outKey.set("GENRE|" + g);
                context.write(outKey, ONE);

                // GENRE_YEAR|2020|Comedies
                if (!releaseYear.isEmpty()) {
                    outKey.set("GENRE_YEAR|" + releaseYear + "|" + g);
                    context.write(outKey, ONE);
                }
            }
        }

        // 3) Contagem de países
        if (!country.isEmpty()) {
            String[] cs = country.split(",\\s*");
            for (String c : cs) {
                if (c.isEmpty()) continue;
                outKey.set("COUNTRY|" + c);
                context.write(outKey, ONE);
            }
        }

        // 4) Rating por tipo (ex: RATING|Movie|TV-14)
        if (!rating.isEmpty() && !type.isEmpty()) {
            outKey.set("RATING|" + type + "|" + rating);
            context.write(outKey, ONE);
        }

        // 5) Duração de filmes em buckets + número de temporadas em séries
        if ("Movie".equals(type)) {
            Integer minutes = parseLeadingInt(duration);
            if (minutes != null) {
                String bucket = durationBucket(minutes); // <90, 90-110, >110
                outKey.set("MOVIE_BUCKET|" + bucket);
                context.write(outKey, ONE);
            }
        } else if ("TV Show".equals(type)) {
            // Normaliza para remover "season"/"seasons" e evitar pegar lixo tipo "s"
            String normDuration = duration.toLowerCase()
                    .replace("seasons", "")
                    .replace("season", "")
                    .trim();

            Integer seasons = parseLeadingInt(normDuration); // "2" -> 2
            if (seasons != null) {
                outKey.set("SEASONS|" + seasons);
                context.write(outKey, ONE);
            }
        }
    }

    // --- Funções auxiliares ---

    private static String safe(String s) {
        return (s == null) ? "" : s.trim();
    }

    private static Integer parseLeadingInt(String s) {
        if (s == null) return null;
        s = s.trim();
        StringBuilder digits = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isDigit(ch)) {
                digits.append(ch);
            } else {
                break;
            }
        }
        if (digits.length() == 0) return null;
        try {
            return Integer.parseInt(digits.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String durationBucket(int minutes) {
        if (minutes < 90) return "<90";
        if (minutes <= 110) return "90-110";
        return ">110";
    }

    /**
     * Parser simples de CSV que respeita aspas.
     * Não é perfeito para todos os casos, mas funciona bem para o formato do netflix_titles.csv.
     */
    private static List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        if (line == null) return result;

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '"') {
                inQuotes = !inQuotes; // alterna
            } else if (ch == ',' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        result.add(current.toString());
        return result;
    }
}
