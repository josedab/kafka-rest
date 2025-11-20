# RFC-0010: Response Compression (gzip/Brotli)

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Enable HTTP response compression (gzip/Brotli) to reduce bandwidth usage by 40-70% for JSON responses.

## Motivation

REST Proxy returns large JSON responses without compression:

**Example response sizes:**
- `GET /v3/clusters/{id}/topics` - 500 topics → 250 KB uncompressed
- `GET /consumers/{group}/records` - 100 records → 1 MB uncompressed
- `GET /v3/clusters/{id}/acls` - 1000 ACLs → 500 KB uncompressed

**Impact:**
- High bandwidth costs (especially cloud egress)
- Slow responses on limited bandwidth
- Poor mobile/edge performance
- Unnecessary network saturation

**With compression:**
- JSON compresses 60-80% (highly repetitive structure)
- Brotli: 65-85% compression
- gzip: 60-75% compression
- **Bandwidth reduction: 40-70%**

## Detailed Design

### Jetty Compression Handler

```java
// KafkaRestApplication.java
@Override
public void configurePreResourceHandling(ServletContextHandler context) {
  if (config.getBoolean("response.compression.enabled")) {
    GzipHandler gzipHandler = new GzipHandler();

    // Configure compression
    gzipHandler.setMinGzipSize(
        config.getInt("response.compression.min.size"));
    gzipHandler.setIncludedMimeTypes(
        "application/json",
        "application/vnd.kafka.json.v2+json",
        "text/plain");
    gzipHandler.setExcludedMimeTypes(
        "application/octet-stream");  // Already compressed

    // Compression level (1-9, default 6)
    gzipHandler.setCompressionLevel(
        config.getInt("response.compression.level"));

    // Add Brotli support
    if (config.getBoolean("response.compression.brotli.enabled")) {
      gzipHandler.addIncludedMethods("GET", "POST");
    }

    context.setGzipHandler(gzipHandler);
  }
}
```

### Brotli Support

```java
// BrotliHandler.java
public class BrotliHandler extends GzipHandler {

  @Override
  protected HttpOutput.Interceptor newGzipInterceptor(
      HttpChannel channel,
      HttpOutput.Interceptor next,
      boolean compress) {

    // Check Accept-Encoding header
    String acceptEncoding = channel.getRequest()
        .getHttpFields()
        .get(HttpHeader.ACCEPT_ENCODING);

    if (acceptEncoding != null && acceptEncoding.contains("br")) {
      // Use Brotli (better compression)
      return new BrotliInterceptor(channel, next);
    } else if (acceptEncoding != null && acceptEncoding.contains("gzip")) {
      // Fall back to gzip
      return super.newGzipInterceptor(channel, next, compress);
    } else {
      // No compression
      return next;
    }
  }
}

class BrotliInterceptor implements HttpOutput.Interceptor {
  private final Encoder encoder;

  public BrotliInterceptor(HttpChannel channel,
                           HttpOutput.Interceptor next) {
    this.encoder = new Encoder.Parameters()
        .setQuality(config.getInt("response.compression.brotli.quality"))
        .setMode(Encoder.Mode.TEXT);

    // Set Content-Encoding header
    channel.getResponse().setHeader("Content-Encoding", "br");
  }

  @Override
  public void write(ByteBuffer content, boolean last, Callback callback) {
    ByteBuffer compressed = encoder.compress(content, last);
    next.write(compressed, last, callback);
  }
}
```

### Configuration

```properties
# Enable response compression
response.compression.enabled=true

# Minimum response size to compress (bytes)
response.compression.min.size=1024

# Compression level (1-9, higher = better compression, slower)
# 6 is good balance
response.compression.level=6

# Enable Brotli (better compression than gzip)
response.compression.brotli.enabled=true
response.compression.brotli.quality=4

# Excluded paths (already compressed data)
response.compression.excluded.paths=/health,/metrics

# Excluded content types
response.compression.excluded.mime.types=application/octet-stream,image/*
```

### Conditional Compression

```java
// CompressionFilter.java
@Provider
@PreMatching
public class CompressionFilter implements ContainerResponseFilter {

  @Override
  public void filter(ContainerRequestContext requestContext,
                    ContainerResponseContext responseContext) {

    // Check if client accepts compression
    String acceptEncoding = requestContext.getHeaderString("Accept-Encoding");
    if (acceptEncoding == null || !acceptEncoding.contains("gzip")) {
      return;  // Client doesn't support compression
    }

    // Check response size
    int contentLength = responseContext.getLength();
    if (contentLength > 0 && contentLength < minCompressSize) {
      return;  // Too small to benefit from compression
    }

    // Check content type
    MediaType mediaType = responseContext.getMediaType();
    if (!isCompressible(mediaType)) {
      return;  // Not compressible
    }

    // Enable compression
    responseContext.setProperty("compression.enabled", true);
  }

  private boolean isCompressible(MediaType mediaType) {
    return mediaType != null && (
        mediaType.toString().startsWith("application/json") ||
        mediaType.toString().startsWith("text/"));
  }
}
```

### Metrics

```java
// Compression metrics
compressionRatio.record((double) uncompressedSize / compressedSize);
compressionTimeMs.record(duration);
compressedResponseCount.increment();
uncompressedResponseCount.increment();
bandwidthSavedBytes.record(uncompressedSize - compressedSize);
```

## Example Usage

### Before

```bash
curl -v http://localhost:8082/v3/clusters/abc/topics

< HTTP/1.1 200 OK
< Content-Type: application/json
< Content-Length: 256834
<
{"kind":"KafkaTopicList","data":[...]}  # 256 KB
```

### After (with gzip)

```bash
curl -v -H "Accept-Encoding: gzip" \
  http://localhost:8082/v3/clusters/abc/topics

< HTTP/1.1 200 OK
< Content-Type: application/json
< Content-Encoding: gzip
< Content-Length: 89234
<
[compressed binary data]  # 89 KB (65% reduction)
```

### After (with Brotli)

```bash
curl -v -H "Accept-Encoding: br, gzip" \
  http://localhost:8082/v3/clusters/abc/topics

< HTTP/1.1 200 OK
< Content-Type: application/json
< Content-Encoding: br
< Content-Length: 76912
<
[compressed binary data]  # 77 KB (70% reduction)
```

### Compression Comparison

| Endpoint | Uncompressed | gzip | Brotli | Savings |
|----------|--------------|------|--------|---------|
| List 500 topics | 256 KB | 89 KB | 77 KB | 70% |
| List 1000 ACLs | 512 KB | 178 KB | 153 KB | 70% |
| Consume 100 records | 1.2 MB | 420 KB | 360 KB | 70% |
| Get cluster info | 2 KB | 2 KB | 2 KB | 0% (below min) |

## Implementation Plan

### Phase 1: Basic gzip (Week 1)

1. Add GzipHandler to Jetty
2. Configure compression settings
3. Unit tests

### Phase 2: Brotli Support (Optional)

1. Add Brotli dependency
2. Implement BrotliHandler
3. Performance testing

## Backwards Compatibility

- **100% compatible** - Uses HTTP standard
- **Client opt-in** - Via Accept-Encoding header
- **Transparent** - Clients decompress automatically

## Alternatives Considered

### Alternative 1: CDN Compression

Use CDN (Cloudflare, CloudFront) for compression.

**Still valuable because:**
- Many deployments don't use CDN
- Origin bandwidth still matters
- Reduces CDN egress costs

### Alternative 2: Binary Protocol

Use Protocol Buffers or MessagePack.

**Rejected because:**
- Breaking API change
- Requires client updates
- JSON is requirement for REST

## Open Questions

1. **Compression level:** 6 (balanced) or 9 (maximum)?
   - Proposal: 6 for production, 9 optional

2. **Brotli quality:** 4 (fast) or 11 (maximum)?
   - Proposal: 4 for real-time, higher for bulk operations

## Success Criteria

- [ ] 40-70% bandwidth reduction
- [ ] < 5ms added latency
- [ ] 100% client compatibility
- [ ] Metrics showing compression ratios

## Effort Estimation

**Total:** 5 dev-days

| Task | Days |
|------|------|
| Jetty gzip integration | 2 |
| Configuration | 1 |
| Testing | 1.5 |
| Documentation | 0.5 |

## Required Approvals

- [ ] Performance Team
