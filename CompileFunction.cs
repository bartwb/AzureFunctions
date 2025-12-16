using System.Diagnostics;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace FunctionsGateway;

public class CompileFunction
{
    private readonly ILogger<CompileFunction> _log;

    public CompileFunction(ILogger<CompileFunction> log)
    {
        _log = log;
    }

    [Function("compile")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "compile")] HttpRequestData req)
    {
        var sw = Stopwatch.StartNew();
        var corr = Guid.NewGuid().ToString("N")[..12];

        _log.LogInformation(
            "COMPILE IN  corr={corr} method={method} path={path} ua={ua}",
            corr,
            req.Method,
            req.Url.AbsolutePath,
            req.Headers.TryGetValues("User-Agent", out var ua) ? string.Join(";", ua) : "-"
        );

        string body;
        try
        {
            body = await new StreamReader(req.Body).ReadToEndAsync();
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "COMPILE ERR corr={corr} reason=read_body_failed", corr);
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync("""{"error":"invalid_body"}""");
            return bad;
        }

        _log.LogInformation(
            "COMPILE BODY corr={corr} bodyLen={len} snippet={snippet}",
            corr,
            body?.Length ?? 0,
            body?.Length > 300 ? body[..300] : body
        );

        if (string.IsNullOrWhiteSpace(body))
        {
            _log.LogWarning("COMPILE BAD corr={corr} reason=empty_body", corr);
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync("""{"error":"body_empty"}""");
            return bad;
        }

        string jobId;
        string operation;

        try
        {
            (jobId, operation) = await JobSubmitter.EnqueueAsync("compile", body);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "COMPILE ERR corr={corr} reason=enqueue_failed", corr);
            var err = req.CreateResponse(HttpStatusCode.InternalServerError);
            await err.WriteStringAsync("""{"error":"enqueue_failed"}""");
            return err;
        }

        sw.Stop();

        _log.LogInformation(
            "COMPILE OK  corr={corr} jobId={jobId} operation={operation} elapsedMs={elapsed}",
            corr,
            jobId,
            operation,
            sw.ElapsedMilliseconds
        );

        var resp = req.CreateResponse(HttpStatusCode.Accepted);
        resp.Headers.Add("Content-Type", "application/json");
        resp.Headers.Add("Location", $"/api/jobs/{operation}/{jobId}");

        await resp.WriteStringAsync(
            $$"""{"jobId":"{{jobId}}","operation":"{{operation}}","statusUrl":"/api/jobs/{{operation}}/{{jobId}}"}"""
        );

        return resp;
    }
}
