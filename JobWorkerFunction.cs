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
        var table = StorageClients.JobsTable();

        JobEntity? job = null;

        try
        {
            // 1) Parse message
            var msg = JsonSerializer.Deserialize<JobMessage>(msgJson)
                      ?? throw new InvalidOperationException("Invalid queue message (not JSON)");

            // 2) Fetch job row
            var jobResp = await table.GetEntityAsync<JobEntity>(msg.Operation, msg.JobId);
            job = jobResp.Value;

            if (job.Status is "Succeeded" or "Failed") return; // idempotent

            // helper: update job row frequently (so you can debug from job-status)
            async Task TouchAsync(string lastStep, string? error = null)
            {
                job!.LastStep = lastStep;
                job.UpdatedUtc = DateTimeOffset.UtcNow;
                if (error != null) job.ErrorMessage = error;

                // Note: UpdateEntityAsync updates ETag inside 'job' only if you re-fetch.
                // But for simple flows this is usually fine; if you hit ETag errors, use Upsert (see note below).
                await table.UpdateEntityAsync(job!, job!.ETag, TableUpdateMode.Replace);
            }

            // 3) Mark Running ASAP
            job.Status = "Running";
            job.Attempts += 1;
            await TouchAsync("Running");

            // 4) Load input from blob
            await TouchAsync("LoadingInput");
            var inputBlob = StorageClients.InputContainer().GetBlobClient(job.InputBlobName);
            var input = (await inputBlob.DownloadContentAsync()).Value.Content.ToString();
            await TouchAsync("InputLoaded");

            // 5) Call runner (ACA Dynamic Sessions)
            await TouchAsync("CallingRunner");
            var (status, body, contentType) = await AcaForwarder.ForwardAsync(msg.Operation, input, job.SessionId);

            job.RunnerHttpStatus = status;
            job.RunnerContentType = contentType;
            job.RunnerSnippet = body is null ? null : (body.Length > 500 ? body[..500] : body);

            await TouchAsync($"RunnerReturned:{status}");

            // 6) Success path
            if (status >= 200 && status < 300)
            {
                await TouchAsync("WritingOutput");
                await StorageClients.OutputContainer().CreateIfNotExistsAsync();

                var outputBlobName = $"{msg.JobId}.json";
                var outBlob = StorageClients.OutputContainer().GetBlobClient(outputBlobName);

                var toStore = (contentType?.Contains("application/json") ?? false)
                    ? body
                    : JsonSerializer.Serialize(new { operation = msg.Operation, ok = true, contentType, body });

                await outBlob.UploadAsync(new BinaryData(Encoding.UTF8.GetBytes(toStore ?? "")), overwrite: true);

                job.OutputBlobName = outputBlobName;
                job.ErrorMessage = null;
                job.Status = "Succeeded";
                await TouchAsync("Succeeded");
                return;
            }

            // 7) Fail path (non-2xx)
            job.Status = "Failed";
            var err = $"ACA returned {status}: {(body?.Length > 1000 ? body[..1000] : body)}";
            await TouchAsync("Failed", err);
        }
        catch (Exception ex)
        {
            // If we already loaded the job record, write failure info there.
            if (job != null)
            {
                try
                {
                    job.Status = "Failed";
                    job.LastStep = "Exception";
                    job.ErrorMessage = ex.ToString();
                    job.UpdatedUtc = DateTimeOffset.UtcNow;
                    await table.UpdateEntityAsync(job, job.ETag, TableUpdateMode.Replace);
                }
                catch
                {
                    // If even this fails, the message will go poison, but at least you can see the exception in Function logs.
                }
            }

            // Re-throw so Functions runtime can move it to poison after retries (useful if it is truly unprocessable).
            throw;
        }
    }
}
