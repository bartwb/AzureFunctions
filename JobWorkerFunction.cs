using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace FunctionsGateway;

public class JobWorkerFunction
{
    private readonly ILogger<JobWorkerFunction> _log;

    public JobWorkerFunction(ILogger<JobWorkerFunction> log)
    {
        _log = log;
    }

    [Function("job-worker")]
    public async Task Run(
        [QueueTrigger("jobs", Connection = "StorageConnection")] string msgJson)
    {
        _log.LogInformation("JOB IN msgLen={len} snippet={snippet}",
            msgJson?.Length ?? 0,
            msgJson?.Length > 300 ? msgJson[..300] : msgJson);

        var table = StorageClients.JobsTable();
        JobEntity? job = null;

        try
        {
            // 1) Parse queue message
            var msg = JsonSerializer.Deserialize<JobMessage>(msgJson)
                      ?? throw new InvalidOperationException("Queue message is not valid JSON");

            _log.LogInformation("JOB STEP parsed operation={operation} jobId={jobId}",
                msg.Operation, msg.JobId);

            // 2) Fetch job
            job = (await table.GetEntityAsync<JobEntity>(msg.Operation, msg.JobId)).Value;

            _log.LogInformation("JOB STEP fetched status={status} attempts={attempts}",
                job.Status, job.Attempts);

            if (job.Status is "Succeeded" or "Failed")
            {
                _log.LogInformation("JOB SKIP already finished");
                return;
            }

            async Task TouchAsync(string step, string? error = null)
            {
                job.LastStep = step;
                job.UpdatedUtc = DateTimeOffset.UtcNow;
                if (error != null)
                    job.ErrorMessage = error;

                // 🔴 CRUCIAAL: Upsert → GEEN ETag conflicts → GEEN poison
                await table.UpsertEntityAsync(job, TableUpdateMode.Replace);
            }

            // 3) Mark running
            job.Status = "Running";
            job.Attempts += 1;
            await TouchAsync("Running");

            // 4) Load input
            await TouchAsync("LoadingInput");
            var inputBlob = StorageClients.InputContainer().GetBlobClient(job.InputBlobName);
            var input = (await inputBlob.DownloadContentAsync()).Value.Content.ToString();
            await TouchAsync("InputLoaded");

            // 5) Call ACA runner
            await TouchAsync("CallingRunner");
            var (status, body, contentType) =
                await AcaForwarder.ForwardAsync(msg.Operation, input, job.SessionId);

            job.RunnerHttpStatus = status;
            job.RunnerContentType = contentType;
            job.RunnerSnippet = body?.Length > 500 ? body[..500] : body;

            await TouchAsync($"RunnerReturned:{status}");

            // 6) Success
            if (status >= 200 && status < 300)
            {
                await TouchAsync("WritingOutput");

                await StorageClients.OutputContainer().CreateIfNotExistsAsync();
                var outBlob = StorageClients.OutputContainer()
                    .GetBlobClient($"{msg.JobId}.json");

                var toStore =
                    contentType?.Contains("application/json") == true
                        ? body
                        : JsonSerializer.Serialize(new
                        {
                            ok = true,
                            status,
                            contentType,
                            body
                        });

                await outBlob.UploadAsync(BinaryData.FromString(toStore ?? ""), overwrite: true);

                job.OutputBlobName = $"{msg.JobId}.json";
                job.Status = "Succeeded";
                job.ErrorMessage = null;

                await TouchAsync("Succeeded");

                _log.LogInformation("JOB OK jobId={jobId}", msg.JobId);
                return;
            }

            // 7) Non-2xx → fail
            job.Status = "Failed";
            await TouchAsync(
                "Failed",
                $"ACA returned {status}: {(body?.Length > 1000 ? body[..1000] : body)}"
            );
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "JOB EXCEPTION");

            if (job != null)
            {
                try
                {
                    job.Status = "Failed";
                    job.LastStep = "Exception";
                    job.ErrorMessage = ex.ToString();
                    job.UpdatedUtc = DateTimeOffset.UtcNow;

                    // 🔴 Ook hier: Upsert
                    await table.UpsertEntityAsync(job, TableUpdateMode.Replace);
                }
                catch
                {
                    // worst case: function logs hebben het
                }
            }

            // opnieuw gooien → runtime retry / poison indien echt kapot
            throw;
        }
    }
}
