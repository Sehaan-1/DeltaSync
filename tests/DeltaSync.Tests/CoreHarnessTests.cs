using DeltaSync.Core.Models;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests;

public class CoreHarnessTests
{
    [Fact]
    public void FileMetadata_CanBeInstantiated_WithValidProperties()
    {
        var now = DateTimeOffset.UtcNow;
        var metadata = new FileMetadata("docs/readme.txt", 1024, "sha256:abc", now);

        metadata.RelativePath.Should().Be("docs/readme.txt");
        metadata.SizeBytes.Should().Be(1024);
        metadata.RootHash.Should().Be("sha256:abc");
        metadata.ModifiedUtc.Should().Be(now);
    }
}
