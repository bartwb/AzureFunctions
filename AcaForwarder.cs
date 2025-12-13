using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;

namespace FunctionsGateway;

public static class AcaForwarder
{
    private static readonly HttpClient _http = new();

    // Dynamic Sessions audience
    private static readonly TokenCredential _cred = new DefaultAzureCredential();
    private static readonly string[] _scopes = new[] { "https://dynamicsessions.io/.default" };

    private static async Task<string> GetBearerAsync()
    {
        var token = await _cred.GetTokenAsync(new TokenRequestContext(_scopes));
        return token.Token;
    }

    public static async Task<(int status, string body, string contentType)> ForwardAsync(
        string action,
        string requestBody,
        string sessionId)
    {
        var poolEndpoint = Environment.GetEnvironmentVariable("POOL_ENDPOINT");
        if (string.IsNullOrWhiteSpace(poolEndpoint))
            throw new InvalidOperationException("POOL_ENDPOINT niet ingesteld");

        using var doc = JsonDocument.Parse(requestBody);
        var root = doc.RootElement;

        // build payload for your ACA /runner contract
        var payload = new
        {
            action,
            code = root.GetProperty("code").GetString(),
            languageVersion = root.TryGetProperty("languageVersion", out var lv) ? lv.GetString() : null,

            candidateId = root.TryGetProperty("candidateId", out var v1) ? v1.GetString() : null,
            candidateName = root.TryGetProperty("candidateName", out var v2) ? v2.GetString() : null,
            candidateEmail = root.TryGetProperty("candidateEmail", out var v3) ? v3.GetString() : null,

            assignmentId = root.TryGetProperty("assignmentId", out var v4) ? v4.GetString() : null,
            assignmentName = root.TryGetProperty("assignmentName", out var v5) ? v5.GetString() : null
        };

        var url = $"{poolEndpoint}/runner?identifier={Uri.EscapeDataString(sessionId)}";
        var json = JsonSerializer.Serialize(payload);

        // Retry on 429 like your Flask version
        var delaySeconds = 1.0;
        for (int attempt = 1; attempt <= 8; attempt++)
        {
            using var msg = new HttpRequestMessage(HttpMethod.Post, url);
            msg.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetBearerAsync());
            msg.Content = new StringContent(json, Encoding.UTF8, "application/json");

            using var resp = await _http.SendAsync(msg, HttpCompletionOption.ResponseContentRead);

            if (resp.StatusCode != (HttpStatusCode)429)
            {
                var body = await resp.Content.ReadAsStringAsync();
                var ct = resp.Content.Headers.ContentType?.ToString() ?? "application/json";
                return ((int)resp.StatusCode, body, ct);
            }

            // 429
            var ra = resp.Headers.RetryAfter?.Delta?.TotalSeconds;
            var sleep = (ra.HasValue && ra.Value > 0) ? ra.Value : delaySeconds;
            await Task.Delay(TimeSpan.FromSeconds(sleep));
            delaySeconds = Math.Min(delaySeconds * 1.8, 12);
        }

        // Last attempt result
        return (429, """{"error":"rate_limited"}""", "application/json");
    }
}
