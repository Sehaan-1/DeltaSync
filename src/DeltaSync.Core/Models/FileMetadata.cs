namespace DeltaSync.Core.Models;

public record FileMetadata(
    string RelativePath,
    long SizeBytes,
    string RootHash,
    DateTimeOffset ModifiedUtc
);
