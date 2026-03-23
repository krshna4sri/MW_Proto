import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.XmlSlurper
import groovy.xml.MarkupBuilder
import java.util.concurrent.*
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * ParallelProcessor.groovy
 *
 * Processes JSON or XML payloads and dispatches each record IN PARALLEL to one
 * or more target endpoints: HTTP REST, SOAP, or OData.
 *
 * Replaces SAP CPI Multicast + Parallel Processing iFlow steps entirely.
 *
 * SAP CPI usage (Groovy Script step):
 *   import com.sap.gateway.ip.core.customdev.util.Message
 *   def Message processData(Message message) {
 *       def body    = message.getBody(String)
 *       def headers = message.getHeaders()          // Map<String,Object>
 *       def result  = ParallelProcessor.process(body, headers as Map<String,String>)
 *       message.setBody(result)
 *       return message
 *   }
 */
class ParallelProcessor {

    // ═══════════════════════════════════════════════════════════════════════
    // ENDPOINT CONFIGURATION
    // Configure target systems here, or pass them in via tags at runtime.
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Build the list of endpoints each record will be dispatched to.
     * Add / remove endpoint blocks as needed.
     * Tags passed into process() can override these at runtime.
     */
    static List<Map> buildEndpoints(Map<String, String> tags) {
        List<Map> endpoints = []

        // ── HTTP REST endpoint ─────────────────────────────────────────────
        if (tags['http.url']) {
            endpoints << [
                type        : 'HTTP',
                url         : tags['http.url'],
                method      : tags['http.method']      ?: 'POST',
                contentType : tags['http.contentType'] ?: 'application/json',
                username    : tags['http.username']    ?: '',
                password    : tags['http.password']    ?: '',
                timeout     : (tags['http.timeout']    ?: '30000').toInteger()
            ]
        }

        // ── SOAP endpoint ──────────────────────────────────────────────────
        if (tags['soap.url']) {
            endpoints << [
                type        : 'SOAP',
                url         : tags['soap.url'],
                soapAction  : tags['soap.action']   ?: '',
                username    : tags['soap.username'] ?: '',
                password    : tags['soap.password'] ?: '',
                timeout     : (tags['soap.timeout'] ?: '30000').toInteger()
            ]
        }

        // ── OData endpoint ─────────────────────────────────────────────────
        if (tags['odata.url']) {
            endpoints << [
                type        : 'ODATA',
                url         : tags['odata.url'],
                entitySet   : tags['odata.entitySet'] ?: '',
                method      : tags['odata.method']    ?: 'POST',
                username    : tags['odata.username']  ?: '',
                password    : tags['odata.password']  ?: '',
                timeout     : (tags['odata.timeout']  ?: '30000').toInteger()
            ]
        }

        // Fallback: if no endpoint tags provided, just return empty list
        // (records will still be processed and returned without dispatching)
        return endpoints
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ENTRY POINT
    // ═══════════════════════════════════════════════════════════════════════

    static String process(String rawPayload, Map<String, String> tags = [:]) {
        String format     = detectFormat(rawPayload, tags)
        List<Map> endpoints = buildEndpoints(tags)

        println "[ParallelProcessor] Format: ${format} | Endpoints: ${endpoints.collect { it.type }}"

        List<Map> items = parseToItems(rawPayload, format, tags)

        // Dispatch all items to all endpoints in parallel, collect dispatch results
        List<Map> dispatchResults = parallelDispatch(items, endpoints, format, tags)

        // Return aggregated dispatch report in the same format as input
        return serializeResults(dispatchResults, format, tags)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // FORMAT DETECTION
    // ═══════════════════════════════════════════════════════════════════════

    static String detectFormat(String payload, Map<String, String> tags) {
        String ct = tags['contentType'] ?: tags['Content-Type'] ?: tags['format'] ?: ''
        if (ct) {
            if (ct.toLowerCase().contains('json')) return 'JSON'
            if (ct.toLowerCase().contains('xml'))  return 'XML'
        }
        String trimmed = payload?.trim() ?: ''
        if (trimmed.startsWith('{') || trimmed.startsWith('[')) return 'JSON'
        if (trimmed.startsWith('<'))                             return 'XML'
        throw new IllegalArgumentException(
            "Cannot detect payload format. Set 'contentType' tag or start payload with { [ or <"
        )
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PARSE INPUT → List<Map>
    // ═══════════════════════════════════════════════════════════════════════

    static List<Map> parseToItems(String rawPayload, String format, Map<String, String> tags) {
        if (format == 'JSON') {
            def parsed = new JsonSlurper().parseText(rawPayload)
            List raw   = (parsed instanceof List) ? parsed : [parsed]
            return raw.collect { it as Map }
        } else {
            def root       = new XmlSlurper().parseText(rawPayload)
            String itemTag = tags['itemTag'] ?: 'item'
            def children   = root."${itemTag}"
            List<Map> items = []
            if (children.size() > 0) {
                children.each { items << xmlNodeToMap(it) }
            } else {
                items << xmlNodeToMap(root)
            }
            return items
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PARALLEL DISPATCH  — each item × each endpoint runs in its own thread
    // ═══════════════════════════════════════════════════════════════════════

    static List<Map> parallelDispatch(List<Map> items,
                                      List<Map> endpoints,
                                      String    format,
                                      Map<String, String> tags) {

        // Build one task per (item, endpoint) combination
        List<Callable> tasks = []
        items.eachWithIndex { item, idx ->
            Map processed = processItem(item, format, tags)

            if (endpoints.isEmpty()) {
                // No endpoints — just collect the processed record
                tasks << ({ ->
                    [
                        recordIndex  : idx,
                        record       : processed,
                        dispatches   : [],
                        overallStatus: 'PROCESSED_ONLY'
                    ]
                } as Callable)
            } else {
                tasks << ({ ->
                    List<Map> dispatchLog = endpoints.collect { ep ->
                        dispatchToEndpoint(processed, ep, format)
                    }
                    boolean allOk = dispatchLog.every { it.httpStatus in 200..299 }
                    [
                        recordIndex  : idx,
                        record       : processed,
                        dispatches   : dispatchLog,
                        overallStatus: allOk ? 'SUCCESS' : 'PARTIAL_FAILURE'
                    ]
                } as Callable)
            }
        }

        int poolSize         = Math.min(tasks.size(), Runtime.getRuntime().availableProcessors() * 2)
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(poolSize, 1))
        List<Future> futures = tasks.collect { pool.submit(it) }

        try {
            return futures.collect { it.get() }
        } finally {
            pool.shutdown()
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ENDPOINT DISPATCHERS
    // ═══════════════════════════════════════════════════════════════════════

    static Map dispatchToEndpoint(Map record, Map endpoint, String inputFormat) {
        try {
            switch (endpoint.type) {
                case 'HTTP':   return sendHttp(record, endpoint, inputFormat)
                case 'SOAP':   return sendSoap(record, endpoint)
                case 'ODATA':  return sendOdata(record, endpoint)
                default:
                    return [type: endpoint.type, httpStatus: -1, error: "Unknown endpoint type"]
            }
        } catch (Exception e) {
            return [type: endpoint.type, httpStatus: -1, error: e.message]
        }
    }

    // ── HTTP REST ──────────────────────────────────────────────────────────
    static Map sendHttp(Map record, Map endpoint, String inputFormat) {
        String body = (inputFormat == 'XML')
            ? mapToSimpleXml('record', record)
            : JsonOutput.toJson(record)

        HttpURLConnection conn = openConnection(endpoint.url as String, endpoint.timeout as int)
        conn.setRequestMethod(endpoint.method as String)
        conn.setRequestProperty('Content-Type', endpoint.contentType as String)
        conn.setRequestProperty('Accept',       endpoint.contentType as String)
        applyBasicAuth(conn, endpoint.username as String, endpoint.password as String)
        conn.setDoOutput(true)

        conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it.write(body) }

        int status   = conn.responseCode
        String resp  = readResponse(conn)
        conn.disconnect()

        println "[HTTP] ${endpoint.url} → ${status}"
        return [type: 'HTTP', url: endpoint.url, httpStatus: status, response: resp]
    }

    // ── SOAP ───────────────────────────────────────────────────────────────
    static Map sendSoap(Map record, Map endpoint) {
        String soapBody = buildSoapEnvelope(record)

        HttpURLConnection conn = openConnection(endpoint.url as String, endpoint.timeout as int)
        conn.setRequestMethod('POST')
        conn.setRequestProperty('Content-Type', 'text/xml;charset=UTF-8')
        conn.setRequestProperty('SOAPAction',   endpoint.soapAction as String)
        applyBasicAuth(conn, endpoint.username as String, endpoint.password as String)
        conn.setDoOutput(true)

        conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it.write(soapBody) }

        int status  = conn.responseCode
        String resp = readResponse(conn)
        conn.disconnect()

        println "[SOAP] ${endpoint.url} → ${status}"
        return [type: 'SOAP', url: endpoint.url, httpStatus: status, response: resp]
    }

    // ── OData ──────────────────────────────────────────────────────────────
    static Map sendOdata(Map record, Map endpoint) {
        // OData v2/v4: POST/PATCH to entity set URL, payload as JSON
        String entityUrl = "${endpoint.url}/${endpoint.entitySet}"
        String body      = JsonOutput.toJson(record)

        HttpURLConnection conn = openConnection(entityUrl, endpoint.timeout as int)
        conn.setRequestMethod(endpoint.method as String)
        conn.setRequestProperty('Content-Type', 'application/json')
        conn.setRequestProperty('Accept',       'application/json')
        conn.setRequestProperty('OData-MaxVersion', '4.0')
        conn.setRequestProperty('OData-Version',    '4.0')
        applyBasicAuth(conn, endpoint.username as String, endpoint.password as String)
        conn.setDoOutput(true)

        conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it.write(body) }

        int status  = conn.responseCode
        String resp = readResponse(conn)
        conn.disconnect()

        println "[OData] ${entityUrl} → ${status}"
        return [type: 'ODATA', url: entityUrl, httpStatus: status, response: resp]
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PER-ITEM BUSINESS LOGIC  — replace body with your transformations
    // ═══════════════════════════════════════════════════════════════════════

    static Map processItem(Map item, String format, Map<String, String> tags) {
        Map result = item.collectEntries { k, v ->
            [k, (v != null) ? v.toString().trim() : '']
        } as Map

        result['_processedAt'] = new Date().format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        result['_format']      = format
        result['_status']      = 'PROCESSED'

        // Tag-driven field renaming: rename_oldName=newName
        tags.findAll { k, v -> k.startsWith('rename_') }.each { k, v ->
            String oldKey = k.replace('rename_', '')
            if (result.containsKey(oldKey)) result[v] = result.remove(oldKey)
        }

        return result
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SERIALIZE RESULTS
    // ═══════════════════════════════════════════════════════════════════════

    static String serializeResults(List<Map> results, String format, Map<String, String> tags) {
        if (format == 'JSON') {
            return JsonOutput.prettyPrint(JsonOutput.toJson(results))
        } else {
            String rootTag = tags['rootTag'] ?: 'dispatchResults'
            String itemTag = tags['itemTag'] ?: 'item'
            return buildXml(rootTag, itemTag, results)
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SOAP ENVELOPE BUILDER
    // ═══════════════════════════════════════════════════════════════════════

    static String buildSoapEnvelope(Map record) {
        StringWriter sw = new StringWriter()
        MarkupBuilder mb = new MarkupBuilder(sw)
        mb.'soapenv:Envelope'(
            'xmlns:soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmlns:dat'    : 'http://example.com/data'
        ) {
            'soapenv:Header'()
            'soapenv:Body' {
                'dat:processRecord' {
                    record.each { k, v ->
                        if (!k.startsWith('@')) { "dat:${k}"(v) }
                    }
                }
            }
        }
        return sw.toString()
    }

    // ═══════════════════════════════════════════════════════════════════════
    // UTILITY HELPERS
    // ═══════════════════════════════════════════════════════════════════════

    static HttpURLConnection openConnection(String urlStr, int timeoutMs) {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
        conn.connectTimeout = timeoutMs
        conn.readTimeout    = timeoutMs
        return conn
    }

    static void applyBasicAuth(HttpURLConnection conn, String username, String password) {
        if (username) {
            String encoded = Base64.encoder.encodeToString(
                "${username}:${password}".getBytes(StandardCharsets.UTF_8)
            )
            conn.setRequestProperty('Authorization', "Basic ${encoded}")
        }
    }

    static String readResponse(HttpURLConnection conn) {
        try {
            InputStream is = (conn.responseCode >= 400) ? conn.errorStream : conn.inputStream
            return is ? new String(is.readAllBytes(), StandardCharsets.UTF_8) : ''
        } catch (Exception e) {
            return "Could not read response: ${e.message}"
        }
    }

    static Map xmlNodeToMap(def node) {
        Map map = [:]
        node.children().each { child ->
            String key = child.name()
            map[key]   = (child.children().size() > 0) ? xmlNodeToMap(child) : child.text()
        }
        node.attributes().each { k, v -> map["@${k}"] = v }
        return map
    }

    static String mapToSimpleXml(String rootTag, Map map) {
        StringWriter sw = new StringWriter()
        new MarkupBuilder(sw)."${rootTag}" {
            map.each { k, v -> if (!k.startsWith('@')) "${k}"(v) }
        }
        return sw.toString()
    }

    static String buildXml(String rootTag, String itemTag, List<Map> items) {
        StringWriter sw = new StringWriter()
        MarkupBuilder mb = new MarkupBuilder(sw)
        mb."${rootTag}" {
            items.each { item ->
                "${itemTag}" {
                    item.each { k, v ->
                        if (!k.startsWith('@')) {
                            if (v instanceof Map) {
                                "${k}" { (v as Map).each { ik, iv -> "${ik}"(iv) } }
                            } else if (v instanceof List) {
                                "${k}" { (v as List).eachWithIndex { lv, li -> "entry${li}"(lv) } }
                            } else {
                                "${k}"(v)
                            }
                        }
                    }
                }
            }
        }
        return sw.toString()
    }

    // ═══════════════════════════════════════════════════════════════════════
    // MAIN — demo / smoke test
    // ═══════════════════════════════════════════════════════════════════════

    static void main(String[] args) {

        // ── Demo 1: JSON → HTTP + OData in parallel ────────────────────────
        println "=== DEMO 1: JSON payload → HTTP + OData (no real server) ==="
        String jsonPayload = '''[
          { "id": "1", "name": "Alice",   "amount": "100.5" },
          { "id": "2", "name": "Bob",     "amount": "200.0" },
          { "id": "3", "name": "Charlie", "amount": "50.0"  }
        ]'''

        Map<String, String> jsonTags = [
            contentType       : 'application/json',
            // Uncomment and fill in real endpoints to actually dispatch:
            // 'http.url'      : 'https://your-rest-api/records',
            // 'http.method'   : 'POST',
            // 'http.username' : 'user',
            // 'http.password' : 'pass',
            // 'odata.url'     : 'https://your-odata-service',
            // 'odata.entitySet': 'Records',
            // 'odata.username': 'user',
            // 'odata.password': 'pass'
        ]

        println process(jsonPayload, jsonTags)

        // ── Demo 2: XML → SOAP in parallel ────────────────────────────────
        println "\n=== DEMO 2: XML payload → SOAP (no real server) ==="
        String xmlPayload = '''<orders>
          <item><id>O1</id><product>Widget</product><qty>10</qty></item>
          <item><id>O2</id><product>Gadget</product><qty>5</qty></item>
        </orders>'''

        Map<String, String> xmlTags = [
            contentType : 'application/xml',
            itemTag     : 'item',
            // Uncomment to dispatch to a real SOAP service:
            // 'soap.url'      : 'https://your-soap-service/endpoint',
            // 'soap.action'   : 'http://example.com/ProcessOrder',
            // 'soap.username' : 'user',
            // 'soap.password' : 'pass'
        ]

        println process(xmlPayload, xmlTags)

        // ── Demo 3: Auto-detect format ─────────────────────────────────────
        println "\n=== DEMO 3: Auto-detect XML ==="
        println process('<data><item><id>99</id><val>AutoDetect</val></item></data>', [itemTag: 'item'])
    }
}

ParallelProcessor.main(args)
