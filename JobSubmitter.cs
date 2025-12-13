using System.Text;
using System.Text.Json;
using Azure.Data.Tables;
using Azure.Storage.Queues.Models;

namespace FunctionsGateway;

public static class JobSubmitter
{
    public static async Task<(string jobId, string operation)> EnqueueAsync(string operation, string requestBody)
    {
        var jobId = Guid.NewGuid().ToString("N");
        var sessionId = $"sess-{jobId}".Substring(0, 12); // herbruikbaar en uniek

        // Ensure resources exist (kan ook eenmalig in deployment; dit is safe-idempotent)
        await StorageClients.JobsQueue().CreateIfNotExistsAsync();
        await StorageClients.JobsTable().CreateIfNotExistsAsync();
        await StorageClients.InputContainer().CreateIfNotExistsAsync();

        // Input naar blob
        var inputBlobName = $"{jobId}.json";
        var inputBlob = StorageClients.InputContainer().GetBlobClient(inputBlobName);
        await inputBlob.UploadAsync(new BinaryData(Encoding.UTF8.GetBytes(requestBody)), overwrite: true);

        // Job entity naar Table
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
            OutputBlobName = "", // wordt gezet bij success
            Attempts = 0
        };

        await StorageClients.JobsTable().AddEntityAsync(entity);

        // Message naar queue (klein houden)
        var msg = JsonSerializer.Serialize(new JobMessage(operation, jobId));
        await StorageClients.JobsQueue().SendMessageAsync(Convert.ToBase64String(Encoding.UTF8.GetBytes(msg)));

        return (jobId, operation);
    }
}
