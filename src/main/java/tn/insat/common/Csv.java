package tn.insat.common;

import java.util.ArrayList;
import java.util.List;

public final class Csv {
    private Csv() {}

    public static String[] split(String line) {
        List<String> out = new ArrayList<>();
        if (line == null) return new String[0];
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                out.add(unquote(sb.toString()));
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(unquote(sb.toString()));
        return out.toArray(new String[0]);
    }

    public static String unquote(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
            t = t.substring(1, t.length() - 1);
        }
        return t;
    }
}
