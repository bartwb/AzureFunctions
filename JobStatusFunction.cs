using System.Net;
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

        // Query params
        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var debug = string.Equals(query.Get("debug"), "1", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(query.Get("debug"), "true", StringComparison.OrdinalIgnoreCase);
        var includeOutput = !(string.Equals(query.Get("includeOutput"), "0", StringComparison.OrdinalIgnoreCase)
                              || string.Equals(query.Get("includeOutput"), "false", StringComparison.OrdinalIgnoreCase));

        // For Queued/Running: return 202
        if (entity.Status is "Queued" or "Running")
        {
            var resp202 = req.CreateResponse(HttpStatusCode.Accepted);
            resp202.Headers.Add("Content-Type", "application/json");

            if (!debug)
            {
                await resp202.WriteStringAsync(
                    $$"""{"jobId":"{{jobId}}","operation":"{{operation}}","status":"{{entity.Status}}"}""");
            }
            else
            {
                await resp202.WriteStringAsync(System.Text.Json.JsonSerializer.Serialize(new
                {
                    jobId,
                    operation,
                    status = entity.Status,

                    // debug fields
                    createdUtc = entity.CreatedUtc,
                    updatedUtc = entity.UpdatedUtc,
                    attempts = entity.Attempts,
                    lastStep = entity.LastStep,
                    lastError = entity.ErrorMessage,
                    runnerHttpStatus = entity.RunnerHttpStatus,
                    runnerContentType = entity.RunnerContentType
                }));
            }

            return resp202;
        }

        // Succeeded/Failed => 200
        var resp = req.CreateResponse(HttpStatusCode.OK);
        resp.Headers.Add("Content-Type", "application/json");

        if (entity.Status == "Failed")
        {
            if (!debug)
            {
                await resp.WriteStringAsync(
                    $$"""{"jobId":"{{jobId}}","operation":"{{operation}}","status":"Failed","error":{{JsonEsc(entity.ErrorMessage)}}}""");
            }
            else
            {
                await resp.WriteStringAsync(System.Text.Json.JsonSerializer.Serialize(new
                {
                    jobId,
                    operation,
                    status = "Failed",
                    error = entity.ErrorMessage,
                    createdUtc = entity.CreatedUtc,
                    updatedUtc = entity.UpdatedUtc,
                    attempts = entity.Attempts,
                    lastStep = entity.LastStep,
                    runnerHttpStatus = entity.RunnerHttpStatus,
                    runnerContentType = entity.RunnerContentType
                }));
            }

            return resp;
        }

        // Succeeded
        if (!includeOutput)
        {
            // Output overslaan, alleen metadata teruggeven
            await resp.WriteStringAsync(System.Text.Json.JsonSerializer.Serialize(new
            {
                jobId,
                operation,
                status = "Succeeded",
                createdUtc = entity.CreatedUtc,
                updatedUtc = entity.UpdatedUtc,
                attempts = entity.Attempts,
                lastStep = entity.LastStep,
                outputBlobName = entity.OutputBlobName
            }));
            return resp;
        }

        // Output uit blob lezen
        if (!string.IsNullOrWhiteSpace(entity.OutputBlobName))
        {
            try
            {
                var outBlob = StorageClients.OutputContainer().GetBlobClient(entity.OutputBlobName);
                var dl = await outBlob.DownloadContentAsync();
                var json = dl.Value.Content.ToString();

                // als output al JSON is, return direct
                await resp.WriteStringAsync(json);
            }
            catch (Exception ex)
            {
                // Als output blob niet leesbaar is, geef debug informatie
                await resp.WriteStringAsync(System.Text.Json.JsonSerializer.Serialize(new
                {
                    jobId,
                    operation,
                    status = "Succeeded",
                    warning = "Could not read output blob",
                    outputBlobName = entity.OutputBlobName,
                    exception = ex.Message,
                    createdUtc = entity.CreatedUtc,
                    updatedUtc = entity.UpdatedUtc,
                    attempts = entity.Attempts,
                    lastStep = entity.LastStep
                }));
            }
        }
        else
        {
            await resp.WriteStringAsync(
                $$"""{"jobId":"{{jobId}}","operation":"{{operation}}","status":"Succeeded"}""");
        }

        return resp;
    }

    private static string JsonEsc(string? s)
        => s is null ? "null" : System.Text.Json.JsonSerializer.Serialize(s);
}
