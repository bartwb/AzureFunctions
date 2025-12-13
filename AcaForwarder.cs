using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace FunctionsGateway;

public static class AcaForwarder
{
    private static readonly HttpClient _http = new();

    public static async Task<(int status, string body)> ForwardAsync(
        string action,
        string requestBody)
    {
        var poolEndpoint = Environment.GetEnvironmentVariable("POOL_ENDPOINT");
        if (string.IsNullOrWhiteSpace(poolEndpoint))
            throw new InvalidOperationException("POOL_ENDPOINT niet ingesteld");

        using var doc = JsonDocument.Parse(requestBody);
        var root = doc.RootElement;

        var payload = new
        {
            action,
            code = root.GetProperty("code").GetString(),
            candidateId = root.TryGetProperty("candidateId", out var v1) ? v1.GetString() : null,
            candidateName = root.TryGetProperty("candidateName", out var v2) ? v2.GetString() : null,
            candidateEmail = root.TryGetProperty("candidateEmail", out var v3) ? v3.GetString() : null,
            assignmentId = root.TryGetProperty("assignmentId", out var v4) ? v4.GetString() : null,
            assignmentName = root.TryGetProperty("assignmentName", out var v5) ? v5.GetString() : null
        };

        var resp = await _http.PostAsync(
            $"{poolEndpoint}/runner",
            new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
        );

        return ((int)resp.StatusCode, await resp.Content.ReadAsStringAsync());
    }
}
