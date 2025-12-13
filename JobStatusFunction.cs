using System.Net;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace FunctionsGateway;

public class JobStatusFunction
{
    [Function("job-status")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "jobs/{operation}/{jobId}")] HttpRequestData req,
        string operation,
        string jobId)
    {
        var table = StorageClients.JobsTable();

        JobEntity entity;
        try
        {
            var r = await table.GetEntityAsync<JobEntity>(operation, jobId);
            entity = r.Value;
        }
        catch
        {
            return req.CreateResponse(HttpStatusCode.NotFound);
        }

        if (entity.Status is "Queued" or "Running")
        {
            var resp202 = req.CreateResponse(HttpStatusCode.Accepted);
            resp202.Headers.Add("Content-Type", "application/json");
            await resp202.WriteStringAsync($$"""{"jobId":"{{jobId}}","operation":"{{operation}}","status":"{{entity.Status}}"}""");
            return resp202;
        }

        // Succeeded/Failed
        var resp = req.CreateResponse(HttpStatusCode.OK);
        resp.Headers.Add("Content-Type", "application/json");

        if (entity.Status == "Failed")
        {
            await resp.WriteStringAsync($$"""{"jobId":"{{jobId}}","operation":"{{operation}}","status":"Failed","error":{{JsonEsc(entity.ErrorMessage)}}}""");
            return resp;
        }

        // Succeeded: output uit blob halen (of alleen outputBlobName teruggeven)
        if (!string.IsNullOrWhiteSpace(entity.OutputBlobName))
        {
            var outBlob = StorageClients.OutputContainer().GetBlobClient(entity.OutputBlobName);
            var dl = await outBlob.DownloadContentAsync();
            var json = dl.Value.Content.ToString();
            await resp.WriteStringAsync(json);
        }
        else
        {
            await resp.WriteStringAsync($$"""{"jobId":"{{jobId}}","operation":"{{operation}}","status":"Succeeded"}""");
        }

        return resp;
    }

    private static string JsonEsc(string? s)
        => s is null ? "null" : System.Text.Json.JsonSerializer.Serialize(s);
}
