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

        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        // Serve static assets from <base parent>/static if present (e.g. /dashboard/static)
        String staticRoot = base;
        try {
            Path basePath = Paths.get(base);
            Path parent = basePath.getParent();
            if (parent != null) staticRoot = parent.resolve("static").toString();
            else staticRoot = base + "/static";
        } catch (Exception e) {
            staticRoot = base + "/static";
        }
        final String staticRootFinal = staticRoot;
        server.createContext("/static/", (ex) -> serveStatic(ex, staticRootFinal));
        server.createContext("/", (ex) -> respondHtml(ex, INDEX_HTML.replace("${BASE}", base)));
        server.createContext("/api/per_sensor", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, "/per_sensor"))));
        server.createContext("/api/daily", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, "/daily"))));
        server.createContext("/api/anomalies", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, "/anomalies"))));
        // Rolling averages endpoint: expects a dir pointing to a folder with CSV (no fixed subfolder)
        server.createContext("/api/rolling", (ex) -> respondJson(ex, readFirstCsv(resolveDirParam(ex, base, ""))));
        server.setExecutor(null);
        System.out.println("Dashboard running at http://localhost:" + port + " (base=" + base + ")");
        server.start();
        // Keep main thread alive so the HTTP server continues running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

    private static void serveStatic(HttpExchange ex, String staticRoot) throws IOException {
        String path = ex.getRequestURI().getPath(); // starts with /static/
        String rel = path.substring("/static/".length());
        Path file = Paths.get(staticRoot).resolve(rel).normalize();
        if (!file.startsWith(Paths.get(staticRoot)) || !Files.exists(file)) {
            byte[] msg = "Not Found".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(404, msg.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(msg); }
            return;
        }
        String contentType = "application/octet-stream";
        if (path.endsWith(".js")) contentType = "application/javascript";
        else if (path.endsWith(".css")) contentType = "text/css";
        else if (path.endsWith(".svg")) contentType = "image/svg+xml";
        byte[] bytes = Files.readAllBytes(file);
        ex.getResponseHeaders().add("Content-Type", contentType + "; charset=UTF-8");
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
            "<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">" +
            "<title>Chicago Air Quality Dashboard</title>" +
            "<style>" +
            "body{font-family:sans-serif;margin:20px;background:#f5f5f5} " +
            "h1{color:#333} h2{color:#555;border-bottom:2px solid #e67;padding-bottom:10px} " +
            ".row{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:20px} " +
            ".card{flex:1 1 45%;background:white;padding:16px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1)} " +
            "table{width:100%;border-collapse:collapse;font-size:13px} " +
            "th{background:#e67;color:white;padding:10px;text-align:left} " +
            "td{padding:8px;border-bottom:1px solid #ddd} tr:hover{background:#f9f9f9} " +
            ".metric{font-size:24px;color:#e67;font-weight:bold} " +
            ".label{font-size:12px;color:#666} " +
            "select{padding:8px;margin-bottom:10px;border:1px solid #ddd;border-radius:4px} " +
            ".full-width{flex:1 1 100%} canvas{width:100%;height:260px;max-height:360px} " +
            "</style></head><body>" +
            "<h1>📊 Chicago Air Quality Dashboard</h1>" +
            "<p>Data from: <code>${BASE}</code></p>" +
            "<div class=\"row\">" +
            "  <div class=\"card\"><h2>📈 Per-Sensor Summary (Top 15)</h2><canvas id=\"perSensorChart\"></canvas><div id=\"perSensorTable\"></div></div>" +
            "  <div class=\"card\"><h2>📅 Daily Averages</h2><canvas id=\"dailyChart\"></canvas><div id=\"dailyTable\"></div></div>" +
            "</div>" +
            "<div class=\"row\">" +
            "  <div class=\"card\"><h2>⚠️ Anomalies (PM2.5/NO2 > mean+2σ)</h2><canvas id=\"anomaliesChart\"></canvas><div id=\"anomaliesTable\"></div></div>" +
            "</div>" +
            "<div class=\"row\">" +
            "  <div class=\"card full-width\"><h2>🔄 Rolling 24-Hour Averages</h2>" +
            "  <label>Select Sensor:</label> <select id=\"sensorSel\" style=\"margin-bottom:10px\"></select>" +
            "  <canvas id=\"rollingChart\"></canvas><div id=\"rollingTable\"></div></div>" +
            "</div>" +
            // include local Chart.js served from /static/chart.min.js
            "<script src=\"/static/chart.min.js\"></script>" +
            "<script>" +
            "const base = '${BASE}';" +
            "let perChart,dailyChart,anomsChart,rollingChart;" +
            "async function getJson(p){const r=await fetch(p);return await r.json()}" +
            "function makeTable(data, limit){" +
            "  if(!data || data.length===0) return '<p style=\"color:#999\">No data available</p>';" +
            "  const rows=data.slice(0,limit);" +
            "  const keys=Object.keys(rows[0]);" +
            "  let html='<table><thead><tr>';" +
            "  keys.forEach(k=>html+='<th>'+k+'</th>');" +
            "  html+='</tr></thead><tbody>';" +
            "  rows.forEach(r=>{html+='<tr>';keys.forEach(k=>{let v=r[k]||'';if(!isNaN(v)&&v!==''){v=parseFloat(v).toFixed(2)};html+='<td>'+v+'</td>'});html+='</tr>'});" +
            "  html+='</tbody></table>';" +
            "  return html;" +
            "}" +
            "function destroyChart(c){ if(c && c.destroy) try{ c.destroy(); }catch(e){} }" +
            "function createBar(ctx,labels,values,label){ return new Chart(ctx,{type:'bar',data:{labels:labels,datasets:[{label:label,data:values,backgroundColor:'rgba(230,100,70,0.7)'}]},options:{responsive:true,plugins:{legend:{display:false}},scales:{x:{ticks:{maxRotation:45,minRotation:30}},y:{beginAtZero:true}}}}); }" +
            "function createLine(ctx,labels,values,label){ return new Chart(ctx,{type:'line',data:{labels:labels,datasets:[{label:label,data:values,borderColor:'rgba(54,162,235,0.9)',backgroundColor:'rgba(54,162,235,0.2)',fill:true}]},options:{responsive:true,scales:{x:{ticks:{maxRotation:45,minRotation:30}},y:{beginAtZero:true}}}}); }" +
            "async function load(){" +
            "  const per=await getJson('/api/per_sensor?dir='+base);" +
            "  const sorted=per.sort((a,b)=>parseFloat(b.pm25_avg||0)-parseFloat(a.pm25_avg||0));" +
            "  document.getElementById('perSensorTable').innerHTML=makeTable(sorted,15);" +
            "  // per-sensor chart" +
            "  const labels=sorted.slice(0,15).map(x=>x.datasourceid||x.sensor_name||'');" +
            "  const vals=sorted.slice(0,15).map(x=>parseFloat(x.pm25_avg||0));" +
            "  destroyChart(perChart); perChart=createBar(document.getElementById('perSensorChart').getContext('2d'),labels,vals,'PM2.5 Avg');" +
            "  const daily=await getJson('/api/daily?dir='+base);" +
            "  document.getElementById('dailyTable').innerHTML=makeTable(daily,30);" +
            "  const dlabels=daily.map(x=>x.date); const dvals=daily.map(x=>parseFloat(x.pm25_avg||0));" +
            "  destroyChart(dailyChart); dailyChart=createLine(document.getElementById('dailyChart').getContext('2d'),dlabels,dvals,'Daily PM2.5 Avg');" +
            "  const anoms=await getJson('/api/anomalies?dir='+base);" +
            "  document.getElementById('anomaliesTable').innerHTML=makeTable(anoms,50);" +
            "  const alabels=anoms.map(x=>x.time); const avals=anoms.map(x=>parseFloat(x.pm25||0));" +
            "  destroyChart(anomsChart); anomsChart=createBar(document.getElementById('anomaliesChart').getContext('2d'),alabels,avals,'Anomaly PM2.5');" +
            "  const roll=await getJson('/api/rolling?dir='+'rolling_out');" +
            "  const sensors=[...new Set(roll.map(x=>x.datasourceid))];" +
            "  const sel=document.getElementById('sensorSel'); sel.innerHTML='';" +
            "  sensors.slice(0,50).forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;sel.appendChild(o);});" +
            "  function renderRolling(sensor){" +
            "    const rows=roll.filter(r=>r.datasourceid===sensor).sort((a,b)=>new Date(a.time)-new Date(b.time));" +
            "    document.getElementById('rollingTable').innerHTML=makeTable(rows,100);" +
            "    const labels=rows.map(r=>r.time); const vals=rows.map(r=>parseFloat(r.rolling_pm25_24h||r.rolling_pm25||0));" +
            "    destroyChart(rollingChart); rollingChart=createLine(document.getElementById('rollingChart').getContext('2d'),labels,vals,'Rolling PM2.5 (24h)');" +
            "  }" +
            "  if(sensors.length){sel.value=sensors[0];renderRolling(sensors[0]);}" +
            "  sel.addEventListener('change',()=>renderRolling(sel.value));" +
            "}" +
            "load();" +
            "</script></body></html>";
}
