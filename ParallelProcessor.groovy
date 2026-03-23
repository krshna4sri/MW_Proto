import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.util.concurrent.*
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.OutputKeys
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList

/**
 * ParallelProcessor.groovy  —  SAP CPI compatible
 *
 * Processes JSON or XML payloads and dispatches each record IN PARALLEL to one
 * or more target endpoints: HTTP REST, SOAP, or OData.
 *
 * Replaces SAP CPI Multicast + Parallel Processing iFlow steps entirely.
 *
 * SAP CPI Groovy Script step usage:
 *   import com.sap.gateway.ip.core.customdev.util.Message
 *   def Message processData(Message message) {
 *       def body    = message.getBody(String)
 *       def headers = message.getHeaders() as Map<String, String>
 *       def result  = ParallelProcessor.process(body, headers)
 *       message.setBody(result)
 *       return message
 *   }
 */
class ParallelProcessor {

    // ═══════════════════════════════════════════════════════════════════════
    // ENDPOINT CONFIGURATION  — driven by tags / iFlow message headers
    // ═══════════════════════════════════════════════════════════════════════

    static List<Map> buildEndpoints(Map<String, String> tags) {
        List<Map> endpoints = []

        if (tags['http.url']) {
            endpoints << [
                type       : 'HTTP',
                url        : tags['http.url'],
                method     : tags['http.method']      ?: 'POST',
                contentType: tags['http.contentType'] ?: 'application/json',
                username   : tags['http.username']    ?: '',
                password   : tags['http.password']    ?: '',
                timeout    : (tags['http.timeout']    ?: '30000').toInteger()
            ]
        }

        if (tags['soap.url']) {
            endpoints << [
                type      : 'SOAP',
                url       : tags['soap.url'],
                soapAction: tags['soap.action']   ?: '',
                username  : tags['soap.username'] ?: '',
                password  : tags['soap.password'] ?: '',
                timeout   : (tags['soap.timeout'] ?: '30000').toInteger()
            ]
        }

        if (tags['odata.url']) {
            endpoints << [
                type     : 'ODATA',
                url      : tags['odata.url'],
                entitySet: tags['odata.entitySet'] ?: '',
                method   : tags['odata.method']    ?: 'POST',
                username : tags['odata.username']  ?: '',
                password : tags['odata.password']  ?: '',
                timeout  : (tags['odata.timeout']  ?: '30000').toInteger()
            ]
        }

        return endpoints
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ENTRY POINT
    // ═══════════════════════════════════════════════════════════════════════

    static String process(String rawPayload, Map<String, String> tags = [:]) {
        String    format    = detectFormat(rawPayload, tags)
        List<Map> endpoints = buildEndpoints(tags)

        println "[ParallelProcessor] Format: ${format} | Endpoints: ${endpoints.collect { it.type }}"

        List<Map> items          = parseToItems(rawPayload, format, tags)
        List<Map> dispatchResult = parallelDispatch(items, endpoints, format, tags)

        return serializeResults(dispatchResult, format, tags)
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
    // PARSE INPUT → List<Map>   (uses javax.xml for XML — no groovy.xml)
    // ═══════════════════════════════════════════════════════════════════════

    static List<Map> parseToItems(String rawPayload, String format, Map<String, String> tags) {
        if (format == 'JSON') {
            def parsed = new JsonSlurper().parseText(rawPayload)
            List raw   = (parsed instanceof List) ? parsed : [parsed]
            return raw.collect { it as Map }
        }

        // XML — parse with javax.xml DocumentBuilder
        String itemTag = tags['itemTag'] ?: 'item'
        Document doc   = parseXmlString(rawPayload)
        NodeList nodes = doc.getElementsByTagName(itemTag)
        List<Map> items = []

        if (nodes.length > 0) {
            for (int i = 0; i < nodes.length; i++) {
                items << elementToMap((Element) nodes.item(i))
            }
        } else {
            // Fall back: treat document root as single item
            items << elementToMap(doc.documentElement)
        }
        return items
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PARALLEL DISPATCH
    // ═══════════════════════════════════════════════════════════════════════

    static List<Map> parallelDispatch(List<Map> items,
                                      List<Map> endpoints,
                                      String    format,
                                      Map<String, String> tags) {
        List<Callable> tasks = []

        items.eachWithIndex { item, idx ->
            Map processed = processItem(item, format, tags)

            if (endpoints.isEmpty()) {
                tasks << ({
                    [recordIndex: idx, record: processed, dispatches: [], overallStatus: 'PROCESSED_ONLY']
                } as Callable)
            } else {
                tasks << ({
                    List<Map> log = endpoints.collect { ep ->
                        dispatchToEndpoint(processed, ep, format)
                    }
                    boolean allOk = log.every { it.httpStatus in 200..299 }
                    [
                        recordIndex  : idx,
                        record       : processed,
                        dispatches   : log,
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
                case 'HTTP':  return sendHttp(record, endpoint, inputFormat)
                case 'SOAP':  return sendSoap(record, endpoint)
                case 'ODATA': return sendOdata(record, endpoint)
                default:      return [type: endpoint.type, httpStatus: -1, error: 'Unknown endpoint type']
            }
        } catch (Exception e) {
            return [type: endpoint.type, httpStatus: -1, error: e.message]
        }
    }

    // ── HTTP REST ──────────────────────────────────────────────────────────
    static Map sendHttp(Map record, Map endpoint, String inputFormat) {
        String body = (inputFormat == 'XML') ? mapToXmlString('record', record) : JsonOutput.toJson(record)

        HttpURLConnection conn = openConnection(endpoint.url as String, endpoint.timeout as int)
        conn.requestMethod = endpoint.method as String
        conn.setRequestProperty('Content-Type', endpoint.contentType as String)
        conn.setRequestProperty('Accept',       endpoint.contentType as String)
        applyBasicAuth(conn, endpoint.username as String, endpoint.password as String)
        conn.doOutput = true
        conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it.write(body) }

        int status  = conn.responseCode
        String resp = readResponse(conn)
        conn.disconnect()
        println "[HTTP] ${endpoint.url} → ${status}"
        return [type: 'HTTP', url: endpoint.url, httpStatus: status, response: resp]
    }

    // ── SOAP ───────────────────────────────────────────────────────────────
    static Map sendSoap(Map record, Map endpoint) {
        String soapBody = buildSoapEnvelope(record)

        HttpURLConnection conn = openConnection(endpoint.url as String, endpoint.timeout as int)
        conn.requestMethod = 'POST'
        conn.setRequestProperty('Content-Type', 'text/xml;charset=UTF-8')
        conn.setRequestProperty('SOAPAction',   endpoint.soapAction as String)
        applyBasicAuth(conn, endpoint.username as String, endpoint.password as String)
        conn.doOutput = true
        conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it.write(soapBody) }

        int status  = conn.responseCode
        String resp = readResponse(conn)
        conn.disconnect()
        println "[SOAP] ${endpoint.url} → ${status}"
        return [type: 'SOAP', url: endpoint.url, httpStatus: status, response: resp]
    }

    // ── OData ──────────────────────────────────────────────────────────────
    static Map sendOdata(Map record, Map endpoint) {
        String entityUrl = "${endpoint.url}/${endpoint.entitySet}"
        String body      = JsonOutput.toJson(record)

        HttpURLConnection conn = openConnection(entityUrl, endpoint.timeout as int)
        conn.requestMethod = endpoint.method as String
        conn.setRequestProperty('Content-Type',     'application/json')
        conn.setRequestProperty('Accept',            'application/json')
        conn.setRequestProperty('OData-MaxVersion', '4.0')
        conn.setRequestProperty('OData-Version',    '4.0')
        applyBasicAuth(conn, endpoint.username as String, endpoint.password as String)
        conn.doOutput = true
        conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it.write(body) }

        int status  = conn.responseCode
        String resp = readResponse(conn)
        conn.disconnect()
        println "[OData] ${entityUrl} → ${status}"
        return [type: 'ODATA', url: entityUrl, httpStatus: status, response: resp]
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PER-ITEM BUSINESS LOGIC  — customise here
    // ═══════════════════════════════════════════════════════════════════════

    static Map processItem(Map item, String format, Map<String, String> tags) {
        Map result = item.collectEntries { k, v ->
            [k, (v != null) ? v.toString().trim() : '']
        } as Map

        result['_processedAt'] = new Date().format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        result['_format']      = format
        result['_status']      = 'PROCESSED'

        // Tag-driven field rename: rename_oldField=newField
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
        }
        String rootTag = tags['rootTag'] ?: 'dispatchResults'
        String itemTag = tags['itemTag'] ?: 'item'
        return buildXmlFromList(rootTag, itemTag, results)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // XML HELPERS  (javax.xml only — no groovy.xml dependency)
    // ═══════════════════════════════════════════════════════════════════════

    /** Parse an XML string into a DOM Document */
    static Document parseXmlString(String xml) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        factory.setNamespaceAware(true)
        return factory.newDocumentBuilder()
                      .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))
    }

    /** Recursively convert a DOM Element → Map */
    static Map elementToMap(Element el) {
        Map map = [:]
        // Attributes
        for (int i = 0; i < el.attributes.length; i++) {
            Node attr = el.attributes.item(i)
            map["@${attr.nodeName}"] = attr.nodeValue
        }
        // Child elements
        NodeList children = el.childNodes
        boolean hasElementChildren = false
        for (int i = 0; i < children.length; i++) {
            Node child = children.item(i)
            if (child.nodeType == Node.ELEMENT_NODE) {
                hasElementChildren = true
                String key    = child.nodeName
                Map childMap  = elementToMap((Element) child)
                // If multiple siblings share the same tag, collect as List
                if (map.containsKey(key)) {
                    def existing = map[key]
                    map[key] = (existing instanceof List) ? existing + [childMap] : [existing, childMap]
                } else {
                    map[key] = childMap
                }
            }
        }
        // Text content (leaf node)
        if (!hasElementChildren) {
            map['_text'] = el.textContent?.trim() ?: ''
            // Flatten: if only _text and no attributes, return plain string via wrapper
            if (map.size() == 1) return ['_text': map['_text']]
        }
        return map
    }

    /** Convert Map → simple XML string using javax.xml DOM */
    static String mapToXmlString(String rootTag, Map map) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        Document doc    = factory.newDocumentBuilder().newDocument()
        Element  root   = doc.createElement(rootTag)
        doc.appendChild(root)
        appendMapToElement(doc, root, map)
        return domToString(doc)
    }

    static void appendMapToElement(Document doc, Element parent, Map map) {
        map.each { k, v ->
            String key = k.toString()
            if (key.startsWith('@')) return   // skip attributes
            Element child = doc.createElement(sanitizeTag(key))
            if (v instanceof Map) {
                appendMapToElement(doc, child, v as Map)
            } else if (v instanceof List) {
                (v as List).eachWithIndex { item, i ->
                    Element entry = doc.createElement("entry${i}")
                    entry.textContent = item?.toString() ?: ''
                    child.appendChild(entry)
                }
            } else {
                child.textContent = v?.toString() ?: ''
            }
            parent.appendChild(child)
        }
    }

    /** Build XML from a list of Maps */
    static String buildXmlFromList(String rootTag, String itemTag, List<Map> items) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        Document doc  = factory.newDocumentBuilder().newDocument()
        Element  root = doc.createElement(rootTag)
        doc.appendChild(root)

        items.each { item ->
            Element itemEl = doc.createElement(itemTag)
            appendMapToElement(doc, itemEl, item)
            root.appendChild(itemEl)
        }
        return domToString(doc)
    }

    /** Build a SOAP envelope XML string using javax.xml DOM */
    static String buildSoapEnvelope(Map record) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        Document doc = factory.newDocumentBuilder().newDocument()

        Element envelope = doc.createElementNS(
            'http://schemas.xmlsoap.org/soap/envelope/', 'soapenv:Envelope')
        envelope.setAttribute('xmlns:soapenv', 'http://schemas.xmlsoap.org/soap/envelope/')
        envelope.setAttribute('xmlns:dat',     'http://example.com/data')
        doc.appendChild(envelope)

        envelope.appendChild(doc.createElementNS(
            'http://schemas.xmlsoap.org/soap/envelope/', 'soapenv:Header'))

        Element body     = doc.createElementNS(
            'http://schemas.xmlsoap.org/soap/envelope/', 'soapenv:Body')
        Element process  = doc.createElementNS('http://example.com/data', 'dat:processRecord')
        record.each { k, v ->
            if (!k.toString().startsWith('@')) {
                Element field = doc.createElementNS('http://example.com/data', "dat:${sanitizeTag(k.toString())}")
                field.textContent = v?.toString() ?: ''
                process.appendChild(field)
            }
        }
        body.appendChild(process)
        envelope.appendChild(body)

        return domToString(doc)
    }

    /** Serialize a DOM Document to a formatted XML string */
    static String domToString(Document doc) {
        StringWriter sw          = new StringWriter()
        TransformerFactory tf    = TransformerFactory.newInstance()
        javax.xml.transform.Transformer transformer = tf.newTransformer()
        transformer.setOutputProperty(OutputKeys.INDENT,               'yes')
        transformer.setOutputProperty(OutputKeys.ENCODING,             'UTF-8')
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, 'no')
        transformer.setOutputProperty('{http://xml.apache.org/xslt}indent-amount', '2')
        transformer.transform(new DOMSource(doc), new StreamResult(sw))
        return sw.toString()
    }

    /** Strip characters not valid in XML tag names */
    static String sanitizeTag(String name) {
        return name.replaceAll('[^a-zA-Z0-9_\\-.]', '_')
                   .replaceWith(~/^([0-9])/, '_$1')
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HTTP UTILITIES
    // ═══════════════════════════════════════════════════════════════════════

    static HttpURLConnection openConnection(String urlStr, int timeoutMs) {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
        conn.connectTimeout   = timeoutMs
        conn.readTimeout      = timeoutMs
        return conn
    }

    static void applyBasicAuth(HttpURLConnection conn, String username, String password) {
        if (username) {
            String encoded = Base64.encoder.encodeToString(
                "${username}:${password}".getBytes(StandardCharsets.UTF_8))
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

    // ═══════════════════════════════════════════════════════════════════════
    // MAIN — smoke test (standalone execution only, not needed in SAP CPI)
    // ═══════════════════════════════════════════════════════════════════════

    static void main(String[] args) {

        // ── JSON → no endpoint (process only) ─────────────────────────────
        println "=== JSON payload (process only, no endpoint) ==="
        String jsonPayload = '''[
          { "id": "1", "name": "Alice",   "amount": "100.5" },
          { "id": "2", "name": "Bob",     "amount": "200.0" },
          { "id": "3", "name": "Charlie", "amount": "50.0"  }
        ]'''
        println process(jsonPayload, [contentType: 'application/json'])

        // ── XML → no endpoint (process only) ──────────────────────────────
        println "\n=== XML payload (process only, no endpoint) ==="
        String xmlPayload = '''<orders>
          <item><id>O1</id><product>Widget</product><qty>10</qty></item>
          <item><id>O2</id><product>Gadget</product><qty>5</qty></item>
        </orders>'''
        println process(xmlPayload, [contentType: 'application/xml', itemTag: 'item'])

        // ── Auto-detect ────────────────────────────────────────────────────
        println "\n=== Auto-detect JSON ==="
        println process('{"id":"99","name":"AutoDetect"}', [:])
    }
}

ParallelProcessor.main(args)
