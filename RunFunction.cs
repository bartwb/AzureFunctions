using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace FunctionsGateway;

public class RunFunction
{
    [Function("run")]
    public async Task<HttpResponseData> RunAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "run")] HttpRequestData req)
    {
        var body = await new StreamReader(req.Body).ReadToEndAsync();
        var (jobId, operation) = await JobSubmitter.EnqueueAsync("run", body);

        var resp = req.CreateResponse(HttpStatusCode.Accepted);
        resp.Headers.Add("Content-Type", "application/json");
        resp.Headers.Add("Location", $"/api/jobs/{operation}/{jobId}");
        await resp.WriteStringAsync($$"""{"jobId":"{{jobId}}","statusUrl":"/api/jobs/{{operation}}/{{jobId}}"}""");
        return resp;
    }
}
