using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace FunctionsGateway;

public static class JobSubmitter
{
    private static int Len(string? s) => string.IsNullOrEmpty(s) ? 0 : s.Length;
    private static string Clip(string? s, int max = 300) =>
        string.IsNullOrEmpty(s) ? "" : (s.Length <= max ? s : s[..max] + "...");

    public static async Task<(string jobId, string operation)> EnqueueAsync(string operation, string requestBody)
    {
        var sw = Stopwatch.StartNew();
        var corr = Guid.NewGuid().ToString("N")[..12];

        var jobId = Guid.NewGuid().ToString("N");
        var sessionId = $"sess-{jobId}".Substring(0, 12);

        Console.WriteLine(
            $"SUBMIT IN  corr={corr} operation='{operation}' jobId='{jobId}' sessionId='{sessionId}' " +
            $"bodyLen={Len(requestBody)} bodySnippet='{Clip(requestBody)}'"
        );

        try
        {
            // Clients (also force evaluation so config errors surface here)
            var q = StorageClients.JobsQueue();
            var t = StorageClients.JobsTable();
            var input = StorageClients.InputContainer();

            // Ensure resources exist (idempotent)
            Console.WriteLine($"SUBMIT STEP corr={corr} step=ensure_resources_start");
            await q.CreateIfNotExistsAsync();
            await t.CreateIfNotExistsAsync();
            await input.CreateIfNotExistsAsync();
            Console.WriteLine($"SUBMIT STEP corr={corr} step=ensure_resources_done elapsedMs={sw.ElapsedMilliseconds}");

            // Upload input to blob
            var inputBlobName = $"{jobId}.json";
            Console.WriteLine($"SUBMIT STEP corr={corr} step=upload_input_start blob='{inputBlobName}'");
            var inputBlob = input.GetBlobClient(inputBlobName);
            await inputBlob.UploadAsync(BinaryData.FromBytes(Encoding.UTF8.GetBytes(requestBody ?? "")), overwrite: true);
            Console.WriteLine($"SUBMIT STEP corr={corr} step=upload_input_done elapsedMs={sw.ElapsedMilliseconds}");

            // Create job entity
            var now = DateTimeOffset.UtcNow;
            var entity = new JobEntity
            {
                PartitionKey = operation,
                RowKey = jobId,
                Status = "Queued",
                CreatedUtc = now,
                UpdatedUtc = now,
                SessionId = sessionId,
                InputBlobName = inputBlobName,
                OutputBlobName = "",
                Attempts = 0,
                LastStep = "Enqueued"
            };

            // Insert into Table
            Console.WriteLine($"SUBMIT STEP corr={corr} step=table_insert_start pk='{operation}' rk='{jobId}'");
            await t.AddEntityAsync(entity);
            Console.WriteLine($"SUBMIT STEP corr={corr} step=table_insert_done elapsedMs={sw.ElapsedMilliseconds}");

            // Send queue message
            var msgJson = JsonSerializer.Serialize(new JobMessage(operation, jobId));
            Console.WriteLine($"SUBMIT STEP corr={corr} step=queue_send_start msgLen={Len(msgJson)} msg='{Clip(msgJson)}'");
            await q.SendMessageAsync(msgJson);
            Console.WriteLine($"SUBMIT STEP corr={corr} step=queue_send_done elapsedMs={sw.ElapsedMilliseconds}");

            sw.Stop();
            Console.WriteLine($"SUBMIT OK  corr={corr} jobId='{jobId}' operation='{operation}' elapsedMs={sw.ElapsedMilliseconds}");

            return (jobId, operation);
        }
        catch (Exception ex)
        {
            sw.Stop();
            Console.WriteLine($"SUBMIT ERR corr={corr} jobId='{jobId}' operation='{operation}' elapsedMs={sw.ElapsedMilliseconds}\n{ex}");
            throw;
        }
    }
}
