import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
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
 * ParallelProcessor.groovy  —  SAP CPI compatible (Java 8 / Groovy 2.x)
 *
 * Processes JSON or XML payloads and dispatches each record IN PARALLEL to
 * HTTP REST, SOAP, or OData endpoints — replaces SAP CPI Multicast step.
 *
 * ── SAP CPI iFlow usage ──────────────────────────────────────────────────
 * Add a Groovy Script step to your iFlow and paste:
 *
 *   import com.sap.gateway.ip.core.customdev.util.Message
 *   def Message processData(Message message) {
 *       def body    = message.getBody(String)
 *       def headers = message.getHeaders().collectEntries { k, v -> [k, v?.toString()] }
 *       message.setBody(ParallelProcessor.process(body, headers as Map))
 *       return message
 *   }
 * ─────────────────────────────────────────────────────────────────────────
 */
class ParallelProcessor {

    // ═══════════════════════════════════════════════════════════════════════
    // ENTRY POINTS  (overloaded — avoids default-param issues in CPI Groovy)
    // ═══════════════════════════════════════════════════════════════════════

    static String process(String rawPayload) {
        return process(rawPayload, new HashMap<String, String>())
    }

    static String process(String rawPayload, Map tags) {
        String    format    = detectFormat(rawPayload, tags)
        List      endpoints = buildEndpoints(tags)

        println "[ParallelProcessor] Format=${format} Endpoints=${endpoints.collect { it.type }}"

        List items   = parseToItems(rawPayload, format, tags)
        List results = parallelDispatch(items, endpoints, format, tags)
        return serializeResults(results, format, tags)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // FORMAT DETECTION
    // ═══════════════════════════════════════════════════════════════════════

    static String detectFormat(String payload, Map tags) {
        String ct = (tags['contentType'] ?: tags['Content-Type'] ?: tags['format'] ?: '').toString()
        if (ct) {
            if (ct.toLowerCase().contains('json')) return 'JSON'
            if (ct.toLowerCase().contains('xml'))  return 'XML'
        }
        String t = payload?.trim() ?: ''
        if (t.startsWith('{') || t.startsWith('[')) return 'JSON'
        if (t.startsWith('<'))                       return 'XML'
        throw new IllegalArgumentException(
            "Cannot detect format. Set 'contentType' tag or start payload with { [ or <")
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ENDPOINT CONFIGURATION  — supplied via iFlow message headers / tags
    // ═══════════════════════════════════════════════════════════════════════

    static List buildEndpoints(Map tags) {
        List endpoints = []

        if (tags['http.url']) {
            endpoints << [
                type       : 'HTTP',
                url        : tags['http.url'].toString(),
                method     : (tags['http.method']      ?: 'POST').toString(),
                contentType: (tags['http.contentType'] ?: 'application/json').toString(),
                username   : (tags['http.username']    ?: '').toString(),
                password   : (tags['http.password']    ?: '').toString(),
                timeout    : Integer.parseInt((tags['http.timeout'] ?: '30000').toString())
            ]
        }

        if (tags['soap.url']) {
            endpoints << [
                type      : 'SOAP',
                url       : tags['soap.url'].toString(),
                soapAction: (tags['soap.action']   ?: '').toString(),
                username  : (tags['soap.username'] ?: '').toString(),
                password  : (tags['soap.password'] ?: '').toString(),
                timeout   : Integer.parseInt((tags['soap.timeout'] ?: '30000').toString())
            ]
        }

        if (tags['odata.url']) {
            endpoints << [
                type     : 'ODATA',
                url      : tags['odata.url'].toString(),
                entitySet: (tags['odata.entitySet'] ?: '').toString(),
                method   : (tags['odata.method']    ?: 'POST').toString(),
                username : (tags['odata.username']  ?: '').toString(),
                password : (tags['odata.password']  ?: '').toString(),
                timeout  : Integer.parseInt((tags['odata.timeout'] ?: '30000').toString())
            ]
        }

        return endpoints
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PARSE INPUT → List<Map>
    // ═══════════════════════════════════════════════════════════════════════

    static List parseToItems(String rawPayload, String format, Map tags) {
        if (format == 'JSON') {
            def parsed = new JsonSlurper().parseText(rawPayload)
            List raw   = (parsed instanceof List) ? parsed : [parsed]
            return raw.collect { it as Map }
        }

        // XML — javax.xml (no groovy.xml dependency)
        String   itemTag = (tags['itemTag'] ?: 'item').toString()
        Document doc     = parseXmlString(rawPayload)
        NodeList nodes   = doc.getElementsByTagName(itemTag)
        List     items   = []

        if (nodes.length > 0) {
            for (int i = 0; i < nodes.length; i++) {
                items << elementToMap((Element) nodes.item(i))
            }
        } else {
            items << elementToMap(doc.documentElement)
        }
        return items
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PARALLEL DISPATCH  — one thread per record
    // ═══════════════════════════════════════════════════════════════════════

    static List parallelDispatch(List items, List endpoints, String format, Map tags) {
        List<Callable> tasks = []

        items.eachWithIndex { item, int idx ->
            final Map processed  = processItem(item as Map, format, tags)
            final List eps       = endpoints
            final String fmt     = format
            final int    recIdx  = idx

            if (eps.isEmpty()) {
                tasks.add({
                    [recordIndex: recIdx, record: processed,
                     dispatches: [], overallStatus: 'PROCESSED_ONLY']
                } as Callable)
            } else {
                tasks.add({
                    List log = eps.collect { ep -> dispatchToEndpoint(processed, ep as Map, fmt) }
                    boolean allOk = log.every { (it as Map).httpStatus in 200..299 }
                    [recordIndex  : recIdx,
                     record       : processed,
                     dispatches   : log,
                     overallStatus: allOk ? 'SUCCESS' : 'PARTIAL_FAILURE']
                } as Callable)
            }
        }

        int poolSize         = Math.min(tasks.size(), Runtime.getRuntime().availableProcessors() * 2)
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(poolSize, 1))
        List<Future> futures = new ArrayList<>()
        for (Callable t : tasks) { futures.add(pool.submit(t)) }

        try {
            List results = new ArrayList<>()
            for (Future f : futures) { results.add(f.get()) }
            return results
        } finally {
            pool.shutdown()
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ENDPOINT DISPATCHERS
    // ═══════════════════════════════════════════════════════════════════════

    static Map dispatchToEndpoint(Map record, Map endpoint, String inputFormat) {
        try {
            String type = endpoint.type.toString()
            if (type == 'HTTP')  return sendHttp(record, endpoint, inputFormat)
            if (type == 'SOAP')  return sendSoap(record, endpoint)
            if (type == 'ODATA') return sendOdata(record, endpoint)
            return [type: type, httpStatus: -1, error: 'Unknown endpoint type']
        } catch (Exception e) {
            return [type: endpoint.type, httpStatus: -1, error: e.getMessage()]
        }
    }

    // ── HTTP REST ──────────────────────────────────────────────────────────
    static Map sendHttp(Map record, Map endpoint, String inputFormat) {
        String body = (inputFormat == 'XML') ? mapToXmlString('record', record) : JsonOutput.toJson(record)
        String url  = endpoint.url.toString()
        int    tout = (endpoint.timeout as int)

        HttpURLConnection conn = openConnection(url, tout)
        conn.setRequestMethod(endpoint.method.toString())
        conn.setRequestProperty('Content-Type', endpoint.contentType.toString())
        conn.setRequestProperty('Accept',       endpoint.contentType.toString())
        applyBasicAuth(conn, endpoint.username.toString(), endpoint.password.toString())
        conn.setDoOutput(true)
        writeBody(conn, body)

        int status  = conn.getResponseCode()
        String resp = readResponse(conn)
        conn.disconnect()
        println "[HTTP] ${url} -> ${status}"
        return [type: 'HTTP', url: url, httpStatus: status, response: resp]
    }

    // ── SOAP ───────────────────────────────────────────────────────────────
    static Map sendSoap(Map record, Map endpoint) {
        String soapBody = buildSoapEnvelope(record)
        String url      = endpoint.url.toString()
        int    tout     = (endpoint.timeout as int)

        HttpURLConnection conn = openConnection(url, tout)
        conn.setRequestMethod('POST')
        conn.setRequestProperty('Content-Type', 'text/xml;charset=UTF-8')
        conn.setRequestProperty('SOAPAction',   endpoint.soapAction.toString())
        applyBasicAuth(conn, endpoint.username.toString(), endpoint.password.toString())
        conn.setDoOutput(true)
        writeBody(conn, soapBody)

        int status  = conn.getResponseCode()
        String resp = readResponse(conn)
        conn.disconnect()
        println "[SOAP] ${url} -> ${status}"
        return [type: 'SOAP', url: url, httpStatus: status, response: resp]
    }

    // ── OData ──────────────────────────────────────────────────────────────
    static Map sendOdata(Map record, Map endpoint) {
        String entityUrl = endpoint.url.toString() + '/' + endpoint.entitySet.toString()
        String body      = JsonOutput.toJson(record)
        int    tout      = (endpoint.timeout as int)

        HttpURLConnection conn = openConnection(entityUrl, tout)
        conn.setRequestMethod(endpoint.method.toString())
        conn.setRequestProperty('Content-Type',     'application/json')
        conn.setRequestProperty('Accept',           'application/json')
        conn.setRequestProperty('OData-MaxVersion', '4.0')
        conn.setRequestProperty('OData-Version',    '4.0')
        applyBasicAuth(conn, endpoint.username.toString(), endpoint.password.toString())
        conn.setDoOutput(true)
        writeBody(conn, body)

        int status  = conn.getResponseCode()
        String resp = readResponse(conn)
        conn.disconnect()
        println "[OData] ${entityUrl} -> ${status}"
        return [type: 'ODATA', url: entityUrl, httpStatus: status, response: resp]
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PER-ITEM BUSINESS LOGIC  — customise here
    // ═══════════════════════════════════════════════════════════════════════

    static Map processItem(Map item, String format, Map tags) {
        Map result = new LinkedHashMap()
        item.each { k, v -> result[k.toString()] = (v != null) ? v.toString().trim() : '' }

        result['_processedAt'] = new Date().format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        result['_format']      = format
        result['_status']      = 'PROCESSED'

        // Tag-driven field rename: rename_oldName -> newName
        tags.each { k, v ->
            String key = k.toString()
            if (key.startsWith('rename_')) {
                String oldField = key.replace('rename_', '')
                if (result.containsKey(oldField)) {
                    result[v.toString()] = result.remove(oldField)
                }
            }
        }
        return result
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SERIALIZE RESULTS
    // ═══════════════════════════════════════════════════════════════════════

    static String serializeResults(List results, String format, Map tags) {
        if (format == 'JSON') {
            return JsonOutput.prettyPrint(JsonOutput.toJson(results))
        }
        String rootTag = (tags['rootTag'] ?: 'dispatchResults').toString()
        String itemTag = (tags['itemTag'] ?: 'item').toString()
        return buildXmlFromList(rootTag, itemTag, results)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // XML HELPERS  — javax.xml DOM only (Java 8 compatible, no groovy.xml)
    // ═══════════════════════════════════════════════════════════════════════

    static Document parseXmlString(String xml) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        factory.setNamespaceAware(true)
        factory.setFeature('http://apache.org/xml/features/disallow-doctype-decl', true)
        return factory.newDocumentBuilder()
                      .parse(new ByteArrayInputStream(xml.getBytes('UTF-8')))
    }

    static Map elementToMap(Element el) {
        Map map = new LinkedHashMap()
        for (int i = 0; i < el.getAttributes().getLength(); i++) {
            Node attr = el.getAttributes().item(i)
            map['@' + attr.getNodeName()] = attr.getNodeValue()
        }
        NodeList children        = el.getChildNodes()
        boolean  hasElementChild = false
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i)
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                hasElementChild = true
                String key   = child.getNodeName()
                Map childMap = elementToMap((Element) child)
                if (map.containsKey(key)) {
                    def existing = map[key]
                    List list    = (existing instanceof List) ? (List) existing : [existing]
                    list.add(childMap)
                    map[key] = list
                } else {
                    map[key] = childMap
                }
            }
        }
        if (!hasElementChild) {
            String text = el.getTextContent()?.trim() ?: ''
            if (map.isEmpty()) return [_text: text]
            map['_text'] = text
        }
        return map
    }

    static String mapToXmlString(String rootTag, Map map) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        Document doc  = factory.newDocumentBuilder().newDocument()
        Element  root = doc.createElement(rootTag)
        doc.appendChild(root)
        appendMapToElement(doc, root, map)
        return domToString(doc)
    }

    static void appendMapToElement(Document doc, Element parent, Map map) {
        map.each { k, v ->
            String key = k.toString()
            if (key.startsWith('@')) return
            Element child = doc.createElement(sanitizeTag(key))
            if (v instanceof Map) {
                appendMapToElement(doc, child, (Map) v)
            } else if (v instanceof List) {
                int i = 0
                for (Object item : (List) v) {
                    Element entry = doc.createElement('entry' + i++)
                    entry.setTextContent(item?.toString() ?: '')
                    child.appendChild(entry)
                }
            } else {
                child.setTextContent(v?.toString() ?: '')
            }
            parent.appendChild(child)
        }
    }

    static String buildXmlFromList(String rootTag, String itemTag, List items) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        Document doc  = factory.newDocumentBuilder().newDocument()
        Element  root = doc.createElement(rootTag)
        doc.appendChild(root)
        for (Object item : items) {
            Element itemEl = doc.createElement(itemTag)
            appendMapToElement(doc, itemEl, (Map) item)
            root.appendChild(itemEl)
        }
        return domToString(doc)
    }

    static String buildSoapEnvelope(Map record) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
        Document doc      = factory.newDocumentBuilder().newDocument()
        String   soapNs   = 'http://schemas.xmlsoap.org/soap/envelope/'
        String   dataNs   = 'http://example.com/data'

        Element envelope = doc.createElementNS(soapNs, 'soapenv:Envelope')
        envelope.setAttribute('xmlns:soapenv', soapNs)
        envelope.setAttribute('xmlns:dat',     dataNs)
        doc.appendChild(envelope)
        envelope.appendChild(doc.createElementNS(soapNs, 'soapenv:Header'))

        Element body    = doc.createElementNS(soapNs, 'soapenv:Body')
        Element process = doc.createElementNS(dataNs,  'dat:processRecord')
        record.each { k, v ->
            if (!k.toString().startsWith('@')) {
                Element field = doc.createElementNS(dataNs, 'dat:' + sanitizeTag(k.toString()))
                field.setTextContent(v?.toString() ?: '')
                process.appendChild(field)
            }
        }
        body.appendChild(process)
        envelope.appendChild(body)
        return domToString(doc)
    }

    static String domToString(Document doc) {
        StringWriter       sw          = new StringWriter()
        TransformerFactory tf          = TransformerFactory.newInstance()
        javax.xml.transform.Transformer t = tf.newTransformer()
        t.setOutputProperty(OutputKeys.INDENT,               'yes')
        t.setOutputProperty(OutputKeys.ENCODING,             'UTF-8')
        t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, 'no')
        t.setOutputProperty('{http://xml.apache.org/xslt}indent-amount', '2')
        t.transform(new DOMSource(doc), new StreamResult(sw))
        return sw.toString()
    }

    static String sanitizeTag(String name) {
        String s = name.replaceAll('[^a-zA-Z0-9_\\-.]', '_')
        return s.matches('^[0-9].*') ? '_' + s : s
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HTTP UTILITIES  — Java 8 compatible (no readAllBytes, no Base64.encoder)
    // ═══════════════════════════════════════════════════════════════════════

    static HttpURLConnection openConnection(String urlStr, int timeoutMs) {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
        conn.setConnectTimeout(timeoutMs)
        conn.setReadTimeout(timeoutMs)
        return conn
    }

    static void applyBasicAuth(HttpURLConnection conn, String username, String password) {
        if (username != null && !username.isEmpty()) {
            String token   = username + ':' + password
            String encoded = Base64.getEncoder().encodeToString(token.getBytes('UTF-8'))
            conn.setRequestProperty('Authorization', 'Basic ' + encoded)
        }
    }

    static void writeBody(HttpURLConnection conn, String body) {
        byte[]       bytes = body.getBytes('UTF-8')
        OutputStream os    = conn.getOutputStream()
        try { os.write(bytes) } finally { os.close() }
    }

    static String readResponse(HttpURLConnection conn) {
        try {
            InputStream is = (conn.getResponseCode() >= 400) ? conn.getErrorStream() : conn.getInputStream()
            if (is == null) return ''
            ByteArrayOutputStream buf = new ByteArrayOutputStream()
            byte[] chunk = new byte[8192]
            int    len
            try {
                while ((len = is.read(chunk)) != -1) { buf.write(chunk, 0, len) }
            } finally { is.close() }
            return buf.toString('UTF-8')
        } catch (Exception e) {
            return 'Could not read response: ' + e.getMessage()
        }
    }
}

// ── SAP CPI iFlow entry point ────────────────────────────────────────────────
// Paste this method in your Groovy Script step (outside the class above):
//
// import com.sap.gateway.ip.core.customdev.util.Message
// def Message processData(Message message) {
//     def body    = message.getBody(String)
//     def headers = message.getHeaders().collectEntries { k, v -> [k, v?.toString()] }
//     message.setBody(ParallelProcessor.process(body, headers as Map))
//     return message
// }
