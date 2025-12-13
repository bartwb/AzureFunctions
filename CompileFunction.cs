using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace FunctionsGateway;

public class CompileFunction
{
    [Function("compile")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "compile")] HttpRequestData req)
    {
        var body = await new StreamReader(req.Body).ReadToEndAsync();
        var sessionId = $"sess-{Guid.NewGuid():N}".Substring(0, 12);
        var (status, response, contentType) = await AcaForwarder.ForwardAsync("compile", body, sessionId);

        var resp = req.CreateResponse((HttpStatusCode)status);
        resp.Headers.Add("Content-Type", contentType);
        var bytes = System.Text.Encoding.UTF8.GetBytes(response ?? "");
        await resp.Body.WriteAsync(bytes, 0, bytes.Length);
        return resp;
    }
}
