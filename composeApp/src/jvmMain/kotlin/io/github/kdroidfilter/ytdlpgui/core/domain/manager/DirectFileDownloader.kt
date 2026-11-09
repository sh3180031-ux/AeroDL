package io.github.kdroidfilter.ytdlpgui.core.domain.manager

import io.github.kdroidfilter.logging.infoln
import io.github.kdroidfilter.logging.warnln
import io.github.kdroidfilter.ytdlp.core.Event
import io.github.kdroidfilter.ytdlp.core.Handle
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import java.io.File
import java.net.URI
import java.net.URLDecoder
import java.net.http.HttpClient
import java.net.http.HttpHeaders
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration

/**
 * Downloads any file (EXE, ZIP, PDF, Google Drive files, ...) over plain HTTP(S),
 * for links that are not video pages handled by yt-dlp.
 *
 * Emits the same [Event]s as yt-dlp downloads so it plugs into [DownloadManager] unchanged.
 */
object DirectFileDownloader {

    private const val USER_AGENT =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"

    /** Extensions that are always downloaded as plain files. Media files are left to yt-dlp. */
    private val FILE_EXTENSIONS = setOf(
        "exe", "msi", "msix", "appx", "bat", "cmd", "jar", "apk", "xapk",
        "zip", "rar", "7z", "tar", "gz", "tgz", "bz2", "xz", "zst", "cab",
        "iso", "img", "bin", "dmg", "pkg", "deb", "rpm", "appimage",
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "rtf", "txt", "csv", "epub",
        "jpg", "jpeg", "png", "gif", "webp", "bmp", "tif", "tiff", "svg", "ico", "psd",
        "torrent", "json", "xml", "srt",
    )

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .connectTimeout(Duration.ofSeconds(30))
            .build()
    }

    // ---------------------------------------------------------------- detection

    /** Google Drive file id from the usual share link shapes, or null. */
    fun googleDriveFileId(url: String): String? {
        val lower = url.lowercase()
        if (!(lower.contains("drive.google.com") || lower.contains("docs.google.com") ||
                lower.contains("drive.usercontent.google.com"))
        ) return null
        if (lower.contains("/folders/")) return null
        return Regex("""/file/d/([\w-]{10,})""").find(url)?.groupValues?.get(1)
            ?: Regex("""[?&]id=([\w-]{10,})""").find(url)?.groupValues?.get(1)
    }

    private fun extensionOf(url: String): String? = runCatching {
        val path = URI(url).path ?: return null
        val name = path.substringAfterLast('/')
        if (!name.contains('.')) null else name.substringAfterLast('.').lowercase()
    }.getOrNull()

    /** Quick check without network: Drive file links and URLs ending in a known file extension. */
    fun looksLikeFile(url: String): Boolean =
        googleDriveFileId(url) != null || extensionOf(url) in FILE_EXTENSIONS

    /**
     * Network check used when yt-dlp is not the right tool: true when the server answers with
     * something that is neither a web page nor audio/video (e.g. application/octet-stream).
     */
    fun probeIsFile(url: String): Boolean = runCatching {
        val req = HttpRequest.newBuilder(URI(resolveDownloadUrl(url)))
            .header("User-Agent", USER_AGENT)
            .timeout(Duration.ofSeconds(20))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .build()
        val resp = client.send(req, HttpResponse.BodyHandlers.discarding())
        if (resp.statusCode() !in 200..299) return@runCatching false
        val type = resp.headers().firstValue("content-type").orElse("").lowercase()
        val attachment = resp.headers().firstValue("content-disposition").orElse("")
            .lowercase().contains("attachment")
        attachment || (type.isNotEmpty() && !type.startsWith("text/html") &&
            !type.startsWith("video/") && !type.startsWith("audio/") &&
            !type.contains("mpegurl") && !type.contains("dash+xml"))
    }.getOrElse {
        warnln { "[DirectFileDownloader] probe failed for $url: ${it.message}" }
        false
    }

    /** Google Drive share links -> direct download link (skips the virus-scan page). */
    fun resolveDownloadUrl(url: String): String =
        googleDriveFileId(url)?.let { "https://drive.usercontent.google.com/download?id=$it&export=download&confirm=t" }
            ?: url

    // ---------------------------------------------------------------- download

    fun download(url: String, targetDir: File, onEvent: (Event) -> Unit): Handle {
        val job = CoroutineScope(Dispatchers.IO).launch {
            var part: File? = null
            try {
                onEvent(Event.Started)
                val req = HttpRequest.newBuilder(URI(resolveDownloadUrl(url)))
                    .header("User-Agent", USER_AGENT)
                    .GET()
                    .build()
                val resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream())
                if (resp.statusCode() !in 200..299) {
                    resp.body().close()
                    onEvent(Event.Error("HTTP ${resp.statusCode()}: the server refused the download"))
                    return@launch
                }
                val type = resp.headers().firstValue("content-type").orElse("").lowercase()
                if (googleDriveFileId(url) != null && type.startsWith("text/html")) {
                    resp.body().close()
                    onEvent(Event.Error("This Google Drive file is private or needs access permission"))
                    return@launch
                }

                val name = fileNameFrom(resp.headers(), resp.uri(), url)
                targetDir.mkdirs()
                val target = uniqueFile(targetDir, name)
                val tmp = File(target.parentFile, target.name + ".part").also { part = it }
                val total = resp.headers().firstValueAsLong("content-length").orElse(-1L)
                infoln { "[DirectFileDownloader] $url -> ${target.absolutePath} ($total bytes)" }
                onEvent(Event.Log("[download] Destination: ${target.absolutePath}"))

                var done = 0L
                var lastTime = System.nanoTime()
                var lastBytes = 0L
                resp.body().use { input ->
                    tmp.outputStream().buffered(1 shl 16).use { out ->
                        val buf = ByteArray(1 shl 16)
                        while (true) {
                            ensureActive()
                            val n = input.read(buf)
                            if (n < 0) break
                            out.write(buf, 0, n)
                            done += n
                            val now = System.nanoTime()
                            if (now - lastTime >= 300_000_000L) {
                                val speed = ((done - lastBytes) * 1_000_000_000L / (now - lastTime))
                                val pct = if (total > 0) done * 100.0 / total else null
                                onEvent(Event.Progress(pct, speed, ""))
                                lastTime = now
                                lastBytes = done
                            }
                        }
                    }
                }
                if (!tmp.renameTo(target)) {
                    tmp.copyTo(target, overwrite = true)
                    tmp.delete()
                }
                part = null
                onEvent(Event.Progress(100.0, null, ""))
                onEvent(Event.Completed(0, true))
            } catch (e: CancellationException) {
                part?.delete()
                onEvent(Event.Cancelled)
                throw e
            } catch (e: Exception) {
                part?.delete()
                onEvent(Event.Error(e.message ?: e.toString(), e))
            }
        }
        return Handle(job)
    }

    private fun fileNameFrom(headers: HttpHeaders, finalUri: URI, originalUrl: String): String {
        val cd = headers.firstValue("content-disposition").orElse("")
        val star = Regex("""filename\*\s*=\s*[^']*'[^']*'([^;]+)""", RegexOption.IGNORE_CASE).find(cd)
        val plain = Regex("""filename\s*=\s*"?([^";]+)"?""", RegexOption.IGNORE_CASE).find(cd)
        val fromHeader = (star ?: plain)?.groupValues?.get(1)?.trim()
            ?.let { URLDecoder.decode(it.replace("+", "%2B"), StandardCharsets.UTF_8) }
        val fromPath = sequenceOf(finalUri.path, runCatching { URI(originalUrl).path }.getOrNull())
            .filterNotNull()
            .map { URLDecoder.decode(it.substringAfterLast('/').replace("+", "%2B"), StandardCharsets.UTF_8) }
            .firstOrNull { it.contains('.') }
        val raw = fromHeader ?: fromPath ?: "download_${System.currentTimeMillis()}"
        return raw.replace(Regex("""[\\/:*?"<>|\r\n]+"""), "_").trim(' ', '.').take(180)
            .ifEmpty { "download_${System.currentTimeMillis()}" }
    }

    private fun uniqueFile(dir: File, name: String): File {
        var f = File(dir, name)
        if (!f.exists()) return f
        val base = name.substringBeforeLast('.', name)
        val ext = if (name.contains('.')) "." + name.substringAfterLast('.') else ""
        var i = 1
        while (f.exists()) f = File(dir, "$base ($i)$ext").also { i++ }
        return f
    }
}
