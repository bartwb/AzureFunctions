using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace FunctionsGateway;

public static class JobSubmitter
{
    private static int Len(string? s) => string.IsNullOrEmpty(s) ? 0 : s.Length;
    private static string Clip(string? s, int max = 300) =>
        string.IsNullOrEmpty(s) ? "" : (s.Length <= max ? s : s[..max] + "...");
    private static string? TryGetString(JsonElement root, params string[] names)
    {
        foreach (var n in names)
        {
            if (root.TryGetProperty(n, out var v) && v.ValueKind == JsonValueKind.String)
                return v.GetString();
        }

        return null;
    }

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
            var backup = StorageClients.BackupContainer();

            // Ensure resources exist (idempotent)
            Console.WriteLine($"SUBMIT STEP corr={corr} step=ensure_resources_start");
            await q.CreateIfNotExistsAsync();
            await t.CreateIfNotExistsAsync();
            await input.CreateIfNotExistsAsync();
            await backup.CreateIfNotExistsAsync();
            Console.WriteLine($"SUBMIT STEP corr={corr} step=ensure_resources_done elapsedMs={sw.ElapsedMilliseconds}");

            // Upload input to blob
            var inputBlobName = $"{jobId}.json";
            Console.WriteLine($"SUBMIT STEP corr={corr} step=upload_input_start blob='{inputBlobName}'");
            var inputBlob = input.GetBlobClient(inputBlobName);
            await inputBlob.UploadAsync(BinaryData.FromBytes(Encoding.UTF8.GetBytes(requestBody ?? "")), overwrite: true);
            Console.WriteLine($"SUBMIT STEP corr={corr} step=upload_input_done elapsedMs={sw.ElapsedMilliseconds}");

            // Capture backup copy with metadata (so input never lost)
            string? backupBlobName = null;
            try
            {
                JsonElement? root = null;
                try
                {
                    using var doc = JsonDocument.Parse(requestBody ?? "{}");
                    root = doc.RootElement.Clone();
                }
                catch (Exception exParse)
                {
                    Console.WriteLine($"SUBMIT WARN corr={corr} reason=backup_parse_failed err='{Clip(exParse.ToString(), 600)}'");
                }

                string? code = null;
                string? candidateId = null;
                string? candidateName = null;
                string? candidateEmail = null;
                string? assignmentId = null;
                string? assignmentName = null;
                string? languageVersion = null;

                if (root.HasValue)
                {
                    code = TryGetString(root.Value, "code", "Code");
                    candidateId = TryGetString(root.Value, "candidateId");
                    candidateName = TryGetString(root.Value, "candidateName");
                    candidateEmail = TryGetString(root.Value, "candidateEmail");
                    assignmentId = TryGetString(root.Value, "assignmentId");
                    assignmentName = TryGetString(root.Value, "assignmentName");
                    languageVersion = TryGetString(root.Value, "languageVersion");
                }

                var backupPayload = new
                {
                    jobId,
                    operation,
                    sessionId,
                    correlationId = corr,
                    createdUtc = DateTimeOffset.UtcNow,
                    code,
                    languageVersion,
                    candidateId,
                    candidateName,
                    candidateEmail,
                    assignmentId,
                    assignmentName,
                    payloadLength = Len(requestBody),
                    rawPayload = requestBody
                };

                backupBlobName = $"{jobId}-backup.json";
                Console.WriteLine($"SUBMIT STEP corr={corr} step=upload_backup_start blob='{backupBlobName}' codePresent={(code != null)}");
                var backupBlob = backup.GetBlobClient(backupBlobName);
                await backupBlob.UploadAsync(BinaryData.FromString(JsonSerializer.Serialize(backupPayload)), overwrite: true);
                Console.WriteLine($"SUBMIT STEP corr={corr} step=upload_backup_done elapsedMs={sw.ElapsedMilliseconds}");
            }
            catch (Exception exBackup)
            {
                Console.WriteLine($"SUBMIT WARN corr={corr} step=upload_backup_failed elapsedMs={sw.ElapsedMilliseconds}\n{Clip(exBackup.ToString(), 900)}");
            }

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
                BackupBlobName = backupBlobName,
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
