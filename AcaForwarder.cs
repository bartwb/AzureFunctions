using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;

namespace FunctionsGateway;

public static class AcaForwarder
{
    private static readonly HttpClient _http = new HttpClient();
    private static readonly TokenCredential _cred = new DefaultAzureCredential();
    private static readonly string[] _scopes = new[] { "https://dynamicsessions.io/.default" };

    private static async Task<string> GetBearerAsync()
    {
        var token = await _cred.GetTokenAsync(new TokenRequestContext(_scopes), default);
        return token.Token;
    }

    private static bool ShouldRetry(HttpStatusCode s) =>
        s == (HttpStatusCode)429 ||
        s == HttpStatusCode.ServiceUnavailable ||
        s == HttpStatusCode.BadGateway ||
        s == HttpStatusCode.GatewayTimeout;

    private static int Len(string? s) => string.IsNullOrEmpty(s) ? 0 : s.Length;
    private static string Clip(string? s, int max = 400) =>
        string.IsNullOrEmpty(s) ? "" : (s.Length <= max ? s : s[..max] + "...");

    public static async Task<(int status, string body, string contentType)> ForwardAsync(
        string action, string requestBody, string sessionId)
    {
        var sw = Stopwatch.StartNew();
        var corr = Guid.NewGuid().ToString("N")[..12];

        var poolEndpoint = Environment.GetEnvironmentVariable("POOL_ENDPOINT");
        Console.WriteLine($"FWD IN   corr={corr} sessionId='{sessionId}' action='{action}' poolSet={!string.IsNullOrWhiteSpace(poolEndpoint)} reqBodyLen={Len(requestBody)}");

        if (string.IsNullOrWhiteSpace(poolEndpoint))
            return (500, """{"error":"POOL_ENDPOINT_not_set"}""", "application/json");

        poolEndpoint = poolEndpoint.TrimEnd('/');

        JsonElement root;
        try
        {
            using var doc = JsonDocument.Parse(requestBody);
            root = doc.RootElement.Clone();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"FWD ERR  corr={corr} reason=json_parse_failed ex='{Clip(ex.ToString(), 800)}'");
            return (400, """{"error":"invalid_json"}""", "application/json");
        }

        string? code = null;
        if (root.TryGetProperty("code", out var c1)) code = c1.GetString();
        else if (root.TryGetProperty("Code", out var c2)) code = c2.GetString();

        var payload = new
        {
            action = action?.Trim(),
            code,
            languageVersion = root.TryGetProperty("languageVersion", out var lv) ? lv.GetString() : null,
            candidateId = root.TryGetProperty("candidateId", out var v1) ? v1.GetString() : null,
            candidateName = root.TryGetProperty("candidateName", out var v2) ? v2.GetString() : null,
            candidateEmail = root.TryGetProperty("candidateEmail", out var v3) ? v3.GetString() : null,
            assignmentId = root.TryGetProperty("assignmentId", out var v4) ? v4.GetString() : null,
            assignmentName = root.TryGetProperty("assignmentName", out var v5) ? v5.GetString() : null
        };

        var url = $"{poolEndpoint}/runner?identifier={Uri.EscapeDataString(sessionId)}";
        var json = JsonSerializer.Serialize(payload);

        Console.WriteLine($"FWD MAP  corr={corr} url='{url}' codePresent={(code != null)} codeLen={Len(code)} jsonLen={Len(json)}");

        var delaySeconds = 1.0;

        for (int attempt = 1; attempt <= 12; attempt++)
        {
            Console.WriteLine($"FWD TRY  corr={corr} attempt={attempt} delayBaseSec={delaySeconds:0.##}");

            string bearer;
            try
            {
                bearer = await GetBearerAsync();
            }
            catch (Exception exTok)
            {
                Console.WriteLine($"FWD ERR  corr={corr} attempt={attempt} reason=token_failed ex='{Clip(exTok.ToString(), 800)}'");
                return (500, """{"error":"token_failed"}""", "application/json");
            }

            using var msg = new HttpRequestMessage(HttpMethod.Post, url);
            msg.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
            msg.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            msg.Headers.Add("x-corr", corr);

            msg.Content = new StringContent(json, Encoding.UTF8);
            msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            HttpResponseMessage resp;
            try
            {
                resp = await _http.SendAsync(msg, HttpCompletionOption.ResponseContentRead);
            }
            catch (Exception exReq)
            {
                Console.WriteLine($"FWD ERR  corr={corr} attempt={attempt} reason=http_send_failed ex='{Clip(exReq.ToString(), 800)}'");
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(delaySeconds, 12)));
                delaySeconds = Math.Min(delaySeconds * 1.8, 12);
                continue;
            }

            var status = (int)resp.StatusCode;
            var ct = resp.Content.Headers.ContentType?.ToString() ?? "application/json";
            var body = await resp.Content.ReadAsStringAsync();

            Console.WriteLine($"FWD RESP corr={corr} attempt={attempt} status={status} ct='{ct}' bodyLen={Len(body)} bodySnippet='{Clip(body)}'");

            if (!ShouldRetry(resp.StatusCode))
                return (status, body, ct);

            var ra = resp.Headers.RetryAfter?.Delta?.TotalSeconds;
            var sleep = (ra.HasValue && ra.Value > 0) ? ra.Value : delaySeconds;

            Console.WriteLine($"FWD RET  corr={corr} attempt={attempt} status={status} sleepSec={sleep:0.##}");

            await Task.Delay(TimeSpan.FromSeconds(sleep));
            delaySeconds = Math.Min(delaySeconds * 1.8, 12);
        }

        sw.Stop();
        Console.WriteLine($"FWD END  corr={corr} reason=rate_limited totalElapsedMs={sw.ElapsedMilliseconds}");
        return (429, """{"error":"rate_limited"}""", "application/json");
    }
}
