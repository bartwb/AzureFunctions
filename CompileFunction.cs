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
        var (status, response) = await AcaForwarder.ForwardAsync("compile", body);

        var resp = req.CreateResponse((HttpStatusCode)status);
        resp.Headers.Add("Content-Type", "application/json");
        var bytes = System.Text.Encoding.UTF8.GetBytes(response ?? "");
        await resp.Body.WriteAsync(bytes, 0, bytes.Length);
        return resp;
    }
}
