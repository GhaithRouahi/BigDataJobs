package tn.insat.dashboard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import tn.insat.common.Csv;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Lightweight dashboard server (no external deps).
 * Serves a simple HTML dashboard and JSON endpoints that read Spark outputs:
 *   /api/per_sensor?dir=<base>   -> expects <base>/per_sensor
 *   /api/daily?dir=<base>        -> expects <base>/daily
 *   /api/anomalies?dir=<base>    -> expects <base>/anomalies
 * Usage: DashboardServer <port> <spark_output_base_dir>
 */
public class DashboardServer {
    public static void main(String[] args) throws IOException {
        int port = args.length >= 1 ? Integer.parseInt(args[0]) : 8080;
        String base = args.length >= 2 ? args[1] : "out/spark";

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", (ex) -> respondHtml(ex, INDEX_HTML.replace("${BASE}", base)));
        server.createContext("/api/per_sensor", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, "/per_sensor"))));
        server.createContext("/api/daily", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, "/daily"))));
        server.createContext("/api/anomalies", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, "/anomalies"))));
        // Rolling averages endpoint: expects a dir pointing to a folder with CSV (no fixed subfolder)
        server.createContext("/api/rolling", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, ""))));
        server.setExecutor(null);
        System.out.println("Dashboard running at http://localhost:" + port + " (base=" + base + ")");
        server.start();
    }

    private static String resolveDirParam(HttpExchange ex, String base, String sub) {
        String q = ex.getRequestURI().getQuery();
        if (q != null && q.startsWith("dir=")) return q.substring(4) + sub;
        return base + sub;
    }

    private static void respondHtml(HttpExchange ex, String html) throws IOException {
        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static void respondJson(HttpExchange ex, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String readFirstCsv(String dirPath) throws IOException {
        Path dir = Paths.get(dirPath);
        if (!Files.exists(dir)) return "[]";
        List<Path> candidates = new ArrayList<Path>();
        for (Path p : (Iterable<Path>) Files.list(dir)::iterator) {
            String name = p.getFileName().toString();
            if (name.endsWith(".csv") || name.startsWith("part-")) {
                candidates.add(p);
            }
        }
        // Try the first non-empty candidate (has at least a header + 1 row)
        for (Path p : candidates) {
            List<String> lines = Files.readAllLines(p);
            if (lines.size() > 1) {
                String header = lines.get(0);
                String[] cols = Csv.split(header);
                List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
                for (int i = 1; i < lines.size(); i++) {
                    String[] vals = Csv.split(lines.get(i));
                    Map<String, String> obj = new LinkedHashMap<String, String>();
                    for (int c = 0; c < Math.min(cols.length, vals.length); c++) {
                        obj.put(Csv.unquote(cols[c]), Csv.unquote(vals[c]));
                    }
                    rows.add(obj);
                    if (rows.size() >= 1000) break; // cap
                }
                return toJson(rows);
            }
        }
        return "[]";
    }

    private static String toJson(List<Map<String, String>> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) sb.append(',');
            Map<String, String> m = rows.get(i);
            sb.append('{');
            int j = 0;
            for (Map.Entry<String, String> e : m.entrySet()) {
                if (j++ > 0) sb.append(',');
                sb.append('"').append(escape(e.getKey())).append('"').append(':')
                        .append('"').append(escape(e.getValue())).append('"');
            }
            sb.append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    private static String escape(String s) { return s == null ? "" : s.replace("\\","\\\\").replace("\"","\\\""); }

    private static final String INDEX_HTML = "<!DOCTYPE html>\n" +
            "<html lang=\\\"en\\\"><head><meta charset=\\\"utf-8\\\">" +
            "<meta name=\\\"viewport\\\" content=\\\"width=device-width, initial-scale=1\\\">" +
            "<title>Chicago Air Quality Dashboard</title>" +
            "<script src=\\\"https://cdn.jsdelivr.net/npm/chart.js\\\"></script>" +
            "<style>body{font-family:sans-serif;margin:20px;} .row{display:flex;gap:24px;flex-wrap:wrap} .card{flex:1 1 480px;border:1px solid #ddd;padding:16px;border-radius:8px}</style>" +
            "</head><body>" +
            "<h2>Chicago Air Quality Dashboard</h2>" +
            "<p>Reading Spark outputs from: <code>${BASE}</code></p>" +
            "<div class=\\\"row\\\">" +
            "<div class=\\\"card\\\"><h3>Top Sensors by PM2.5 Avg</h3><canvas id=\\\"perSensor\\\"></canvas></div>" +
            "<div class=\\\"card\\\"><h3>Daily PM2.5/NO2 Averages</h3><canvas id=\\\"daily\\\"></canvas></div>" +
            "</div>" +
            "<div class=\\\"card\\\"><h3>Anomalies (first 50)</h3><pre id=\\\"anoms\\\"></pre></div>" +
            "<div class=\\\"card\\\"><h3>Rolling 24h Averages <small>(sensor)</small></h3>" +
            "<select id=\\\"sensorSel\\\" style=\\\"margin-bottom:8px\\\"></select>" +
            "<canvas id=\\\"rolling\\\"></canvas></div>" +
            "<script>\n" +
            "const base = '${BASE}';\n" +
            "async function getJson(p){const r=await fetch(p);return await r.json()}\n" +
            "async function load(){\n" +
            " const per=await getJson('/api/per_sensor?dir='+base);\n" +
            " const top=per.sort((a,b)=>parseFloat(b.pm25_avg||0)-parseFloat(a.pm25_avg||0)).slice(0,10);\n" +
            " new Chart(document.getElementById('perSensor'),{type:'bar',data:{labels:top.map(x=>x.datasourceid),datasets:[{label:'PM2.5 Avg',data:top.map(x=>+x.pm25_avg||0),backgroundColor:'#e67'}]}});\n" +
            " const daily=await getJson('/api/daily?dir='+base);\n" +
            " new Chart(document.getElementById('daily'),{type:'line',data:{labels:daily.map(x=>x.date),datasets:[{label:'PM2.5',data:daily.map(x=>+x.pm25_avg||0),borderColor:'#e67',fill:false},{label:'NO2',data:daily.map(x=>+x.no2_avg||0),borderColor:'#36c',fill:false}]}});\n" +
            " const an=await getJson('/api/anomalies?dir='+base);\n" +
            " document.getElementById('anoms').textContent=an.slice(0,50).map(x=>JSON.stringify(x)).join('\n');\n" +
            " // Rolling averages: default dir 'rolling_out' on host; change ?dir= param to override\n" +
            " const roll=await getJson('/api/rolling?dir='+'rolling_out');\n" +
            " const sensors=[...new Set(roll.map(x=>x.datasourceid))];\n" +
            " const sel=document.getElementById('sensorSel');\n" +
            " sensors.forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;sel.appendChild(o);});\n" +
            " function render(sensor){\n" +
            "  const rows=roll.filter(r=>r.datasourceid===sensor).sort((a,b)=>new Date(a.time)-new Date(b.time));\n" +
            "  const labels=rows.map(r=>r.time);\n" +
            "  const pm=rows.map(r=>+r.rolling_pm25_24h||0);\n" +
            "  const no2=rows.map(r=>+r.rolling_no2_24h||0);\n" +
            "  if(window._rollChart) window._rollChart.destroy();\n" +
            "  window._rollChart=new Chart(document.getElementById('rolling'),{type:'line',data:{labels:labels,datasets:[{label:'PM2.5 (24h)',data:pm,borderColor:'#e67',fill:false},{label:'NO2 (24h)',data:no2,borderColor:'#36c',fill:false}]}},{responsive:true,maintainAspectRatio:false});\n" +
            " }\n" +
            " if(sensors.length){sel.value=sensors[0];render(sensors[0]);}\n" +
            " sel.addEventListener('change',()=>render(sel.value));\n" +
            "}\n" +
            "load();\n" +
            "</script></body></html>";
}
