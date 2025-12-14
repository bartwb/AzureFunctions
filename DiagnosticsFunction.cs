using System.Diagnostics;
using System.Net;
using System.Text.Json;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Azure.Core;
using Azure.Identity;
using System.Net.Http.Headers;

namespace FunctionsGateway;

public class DiagnosticsFunction
{
    private static readonly HttpClient _http = new HttpClient();

    private static readonly TokenCredential _cred = new DefaultAzureCredential();
    private static readonly string[] _scopes = new[] { "https://dynamicsessions.io/.default" };

    private static async Task<string> GetBearerAsync()
    {
        var token = await _cred.GetTokenAsync(new TokenRequestContext(_scopes), default);
        return token.Token;
    }

    [Function("diagnostics")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "diagnostics")] HttpRequestData req)
    {
        // Query params
        var qs = req.Url.Query ?? "";
        bool pingRunner = qs.Contains("pingRunner=1") || qs.Contains("pingRunner=true");
        bool pingQueues  = qs.Contains("pingQueues=1")  || qs.Contains("pingQueues=true");

        var result = new Dictionary<string, object?>();

        // 1) Settings presence (no secrets, only "set/not set")
        result["POOL_ENDPOINT_set"] = !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("POOL_ENDPOINT"));
        result["StorageConnection_set"] = !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("StorageConnection"));
        result["AzureWebJobsStorage_set"] = !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
        result["FUNCTIONS_WORKER_RUNTIME"] = Environment.GetEnvironmentVariable("FUNCTIONS_WORKER_RUNTIME");

        // 2) Queue diagnostics (counts)
        if (pingQueues)
        {
            result["queues"] = await GetQueueDiagnosticsAsync();
        }

        // 3) Runner ping (proves whether session endpoint is reachable / waking up)
        if (pingRunner)
        {
            result["runnerPing"] = await PingRunnerAsync();
        }

        var resp = req.CreateResponse(HttpStatusCode.OK);
        resp.Headers.Add("Content-Type", "application/json");
        await resp.WriteStringAsync(JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));
        return resp;
    }

    private static async Task<object> GetQueueDiagnosticsAsync()
    {
        var conn = Environment.GetEnvironmentVariable("StorageConnection");
        if (string.IsNullOrWhiteSpace(conn))
        {
            return new { error = "StorageConnection not set" };
        }

        try
        {
            var jobs = new QueueClient(conn, "jobs");
            var poison = new QueueClient(conn, "jobs-poison");

            await jobs.CreateIfNotExistsAsync();
            await poison.CreateIfNotExistsAsync();

            var jobsProps = await jobs.GetPropertiesAsync();
            var poisonProps = await poison.GetPropertiesAsync();

            return new
            {
                jobs = new { approxMessageCount = jobsProps.Value.ApproximateMessagesCount },
                jobsPoison = new { approxMessageCount = poisonProps.Value.ApproximateMessagesCount }
            };
        }
        catch (Exception ex)
        {
            return new { error = "Queue diagnostics failed", exception = ex.Message };
        }
    }

    private static async Task<object> PingRunnerAsync()
    {
        var poolEndpoint = Environment.GetEnvironmentVariable("POOL_ENDPOINT");
        if (string.IsNullOrWhiteSpace(poolEndpoint))
        {
            return new { error = "POOL_ENDPOINT not set" };
        }

        // We do a very lightweight call that should prove reachability.
        // Prefer a dedicated /health endpoint if your runner has it.
        var healthUrl = $"{poolEndpoint}/healthstatus";
        var sw = Stopwatch.StartNew();

        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Get, healthUrl);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetBearerAsync());
            using var resp = await _http.SendAsync(req);
            var body = await resp.Content.ReadAsStringAsync();
            sw.Stop();

            return new
            {
                url = healthUrl,
                status = (int)resp.StatusCode,
                elapsedMs = sw.ElapsedMilliseconds,
                bodySnippet = body.Length > 500 ? body[..500] : body
            };
        }
        catch (Exception ex)
        {
            sw.Stop();
            return new
            {
                url = healthUrl,
                elapsedMs = sw.ElapsedMilliseconds,
                error = "Runner ping failed",
                exception = ex.Message
            };
        }
    }
}
