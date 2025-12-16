using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker;

namespace FunctionsGateway;

public class JobWorkerFunction
{
    [Function("job-worker")]
    public async Task Run([QueueTrigger("jobs", Connection = "StorageConnection")] string msgJson)
    {
        static int Len(string? s) => string.IsNullOrEmpty(s) ? 0 : s.Length;
        static string Clip(string? s, int max = 400) =>
            string.IsNullOrEmpty(s) ? "" : (s.Length <= max ? s : s[..max] + "...");

        Console.WriteLine($"JOB IN  msgLen={Len(msgJson)} snippet='{Clip(msgJson, 300)}'");

        var table = StorageClients.JobsTable();
        JobEntity? job = null;

        // helper: upsert job row (no ETag conflicts)
        async Task TouchAsync(string step, string? error = null)
        {
            if (job == null) return;

            job.LastStep = step;
            job.UpdatedUtc = DateTimeOffset.UtcNow;
            if (error != null) job.ErrorMessage = error;

            await table.UpsertEntityAsync(job, TableUpdateMode.Replace);
        }

        try
        {
            if (string.IsNullOrWhiteSpace(msgJson))
            {
                Console.WriteLine("JOB BAD reason=empty_msgJson -> drop");
                return; // niets om te retry'en
            }

            var msg = JsonSerializer.Deserialize<JobMessage>(msgJson);
            if (msg == null || string.IsNullOrWhiteSpace(msg.Operation) || string.IsNullOrWhiteSpace(msg.JobId))
            {
                Console.WriteLine($"JOB BAD reason=invalid_message msg='{Clip(msgJson)}' -> drop");
                return; // niet retry'en, dit wordt nooit goed
            }

            Console.WriteLine($"JOB STEP parsed operation='{msg.Operation}' jobId='{msg.JobId}'");

            job = (await table.GetEntityAsync<JobEntity>(msg.Operation, msg.JobId)).Value;
            Console.WriteLine($"JOB STEP fetched status='{job.Status}' attempts={job.Attempts} sessionId='{job.SessionId}' inputBlob='{job.InputBlobName}'");

            if (job.Status is "Succeeded" or "Failed")
            {
                Console.WriteLine("JOB SKIP already finished");
                return;
            }

            // mark running
            job.Status = "Running";
            job.Attempts += 1;
            await TouchAsync("Running");

            // load input
            await TouchAsync("LoadingInput");
            var inputBlob = StorageClients.InputContainer().GetBlobClient(job.InputBlobName);
            var input = (await inputBlob.DownloadContentAsync()).Value.Content.ToString();
            await TouchAsync("InputLoaded");

            // call runner
            await TouchAsync("CallingRunner");
            var (status, body, contentType) = await AcaForwarder.ForwardAsync(msg.Operation, input, job.SessionId);

            job.RunnerHttpStatus = status;
            job.RunnerContentType = contentType;
            job.RunnerSnippet = body?.Length > 500 ? body[..500] : body;
            await TouchAsync($"RunnerReturned:{status}");

            // success
            if (status >= 200 && status < 300)
            {
                await TouchAsync("WritingOutput");
                await StorageClients.OutputContainer().CreateIfNotExistsAsync();

                var outputBlobName = $"{msg.JobId}.json";
                var outBlob = StorageClients.OutputContainer().GetBlobClient(outputBlobName);

                var toStore = (contentType?.Contains("application/json") ?? false)
                    ? body
                    : JsonSerializer.Serialize(new { operation = msg.Operation, ok = true, contentType, body });

                await outBlob.UploadAsync(BinaryData.FromString(toStore ?? ""), overwrite: true);

                job.OutputBlobName = outputBlobName;
                job.ErrorMessage = null;
                job.Status = "Succeeded";
                await TouchAsync("Succeeded");

                Console.WriteLine($"JOB OK  jobId='{msg.JobId}'");
                return;
            }

            // non-2xx => fail but DO NOT throw (no poison loop)
            job.Status = "Failed";
            var err = $"ACA returned {status}: {(body?.Length > 1000 ? body[..1000] : body)}";
            await TouchAsync("Failed", err);

            Console.WriteLine($"JOB FAIL jobId='{msg.JobId}' status={status}");
            return;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"JOB ERR ex:\n{ex}");

            if (job != null)
            {
                try
                {
                    job.Status = "Failed";
                    await TouchAsync("Exception", ex.ToString());
                }
                catch (Exception ex2)
                {
                    Console.WriteLine($"JOB ERR (failed to persist failure) ex2:\n{ex2}");
                }
            }

            // Cruciaal: niet rethrowen -> geen poison storm
            return;
        }
    }
}
