using System.Net;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker;

namespace FunctionsGateway;

public class JobWorkerFunction
{
    [Function("job-worker")]
    public async Task Run([QueueTrigger("jobs", Connection = "StorageConnection")] string msgJson)
    {
        var msg = JsonSerializer.Deserialize<JobMessage>(msgJson)
                ?? throw new InvalidOperationException("Invalid queue message");

        var table = StorageClients.JobsTable();

        // Fetch job
        var jobResp = await table.GetEntityAsync<JobEntity>(msg.Operation, msg.JobId);
        var job = jobResp.Value;

        if (job.Status is "Succeeded" or "Failed") return; // idempotent

        // Mark running
        job.Status = "Running";
        job.UpdatedUtc = DateTimeOffset.UtcNow;
        job.Attempts += 1;
        await table.UpdateEntityAsync(job, job.ETag, TableUpdateMode.Replace);

        // Load input from blob
        var inputBlob = StorageClients.InputContainer().GetBlobClient(job.InputBlobName);
        var input = (await inputBlob.DownloadContentAsync()).Value.Content.ToString();

        // Call ACA via jouw forwarder
        var (status, body, contentType) = await AcaForwarder.ForwardAsync(msg.Operation, input, job.SessionId);

        if (status >= 200 && status < 300)
        {
            await StorageClients.OutputContainer().CreateIfNotExistsAsync();

            var outputBlobName = $"{msg.JobId}.json";
            var outBlob = StorageClients.OutputContainer().GetBlobClient(outputBlobName);

            // Bewaar output als JSON. Als ACA geen JSON teruggeeft, wrap je het.
            var toStore = (contentType?.Contains("application/json") ?? false)
                ? body
                : JsonSerializer.Serialize(new { operation = msg.Operation, ok = true, contentType, body });

            await outBlob.UploadAsync(new BinaryData(Encoding.UTF8.GetBytes(toStore ?? "")), overwrite: true);

            job.Status = "Succeeded";
            job.OutputBlobName = outputBlobName;
            job.ErrorMessage = null;
            job.UpdatedUtc = DateTimeOffset.UtcNow;
            await table.UpdateEntityAsync(job, job.ETag, TableUpdateMode.Replace);
        }
        else
        {
            job.Status = "Failed";
            job.ErrorMessage = $"ACA returned {status}: {(body?.Length > 1000 ? body[..1000] : body)}";
            job.UpdatedUtc = DateTimeOffset.UtcNow;
            await table.UpdateEntityAsync(job, job.ETag, TableUpdateMode.Replace);
        }
    }
}
