using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;

namespace FunctionsGateway;

public static class StorageClients
{
    public static string Conn =>
        Environment.GetEnvironmentVariable("StorageConnection")
        ?? throw new InvalidOperationException("StorageConnection niet ingesteld");

    public static QueueClient JobsQueue() => new QueueClient(Conn, "jobs");
    public static TableClient JobsTable() => new TableClient(Conn, "Jobs");
    public static BlobContainerClient InputContainer() => new BlobContainerClient(Conn, "job-input");
    public static BlobContainerClient OutputContainer() => new BlobContainerClient(Conn, "job-output");
}
