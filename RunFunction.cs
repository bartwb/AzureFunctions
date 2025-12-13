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
        var sessionId = $"sess-{Guid.NewGuid():N}".Substring(0, 12);
        var (status, response, contentType) = await AcaForwarder.ForwardAsync("run", body, sessionId);

        var resp = req.CreateResponse((HttpStatusCode)status);
        resp.Headers.Add("Content-Type", contentType);
        var bytes = System.Text.Encoding.UTF8.GetBytes(response ?? "");
        await resp.Body.WriteAsync(bytes, 0, bytes.Length);
        
        return resp;
    }
}
