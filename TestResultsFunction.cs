using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace FunctionsGateway;

public class TestResultsFunction
{
    [Function("test-results")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "results/{operation}/{jobId}")] HttpRequestData req,
        string operation,
        string jobId)
    {
        var table = StorageClients.JobsTable();

        JobEntity entity;
        try
        {
            entity = (await table.GetEntityAsync<JobEntity>(operation, jobId)).Value;
        }
        catch
        {
            return req.CreateResponse(HttpStatusCode.NotFound);
        }

        var blobName = entity.ResultBlobName ?? entity.OutputBlobName;
        var container = entity.ResultBlobName != null
            ? StorageClients.TestResultsContainer()
            : StorageClients.OutputContainer();

        if (string.IsNullOrWhiteSpace(blobName))
        {
            var respMissing = req.CreateResponse(HttpStatusCode.NotFound);
            await respMissing.WriteStringAsync($$"""{"error":"result_not_available","jobId":"{{jobId}}","operation":"{{operation}}"}""");
            return respMissing;
        }

        try
        {
            var blob = container.GetBlobClient(blobName);
            var dl = await blob.DownloadContentAsync();

            var resp = req.CreateResponse(HttpStatusCode.OK);
            var contentType = dl.Value.Details.ContentType ?? "application/json";
            resp.Headers.Add("Content-Type", contentType);
            await resp.WriteStringAsync(dl.Value.Content.ToString());
            return resp;
        }
        catch (Exception ex)
        {
            var respErr = req.CreateResponse(HttpStatusCode.BadGateway);
            await respErr.WriteStringAsync(System.Text.Json.JsonSerializer.Serialize(new
            {
                error = "read_failed",
                jobId,
                operation,
                message = ex.Message
            }));
            return respErr;
        }
    }
}
