using System.Net;
using System.Text;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace FunctionsGateway;

public class HealthFunction
{
    [Function("health")]
    public static async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "health")] HttpRequestData req)
    {
        var resp = req.CreateResponse(HttpStatusCode.OK);
        resp.Headers.Add("Content-Type", "application/json");

        var bytes = Encoding.UTF8.GetBytes("""{"status":"ok"}""");
        await resp.Body.WriteAsync(bytes, 0, bytes.Length);

        return resp;
    }
}
