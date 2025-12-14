using Azure;
using Azure.Data.Tables;

namespace FunctionsGateway;

public record JobMessage(string Operation, string JobId);

public class JobEntity : ITableEntity
{
    // PartitionKey = operation (compile/run/analyse)
    public string PartitionKey { get; set; } = default!;
    // RowKey = jobId
    public string RowKey { get; set; } = default!;

    public string Status { get; set; } = "Queued"; // Queued|Running|Succeeded|Failed
    public DateTimeOffset CreatedUtc { get; set; }
    public DateTimeOffset UpdatedUtc { get; set; }

    public string SessionId { get; set; } = default!; // jouw sess-... id
    public string InputBlobName { get; set; } = default!;   // jobId.json
    public string OutputBlobName { get; set; } = default!;  // jobId.json (optioneel)
    public string? ErrorMessage { get; set; }

    public int Attempts { get; set; } = 0;

    public ETag ETag { get; set; }
    public DateTimeOffset? Timestamp { get; set; }

    // Debugging
    public string? LastStep { get; set; }
    public int? RunnerHttpStatus { get; set; }
    public string? RunnerContentType { get; set; }
    public string? RunnerSnippet { get; set; }
}
