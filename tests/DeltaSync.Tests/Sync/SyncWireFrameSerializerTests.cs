using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync.Wire;
using FluentAssertions;
using System.Security.Cryptography;
using Xunit;

namespace DeltaSync.Tests.Sync;

public class SyncWireFrameSerializerTests
{
    private static readonly Guid TestClusterId = Guid.NewGuid();

    [Fact]
    public void MerkleRootProbe_Roundtrip_PreservesData()
    {
        var probe = new MerkleRootProbe(TestClusterId, "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789");
        ulong correlationId = 42;

        var bytes = SyncWireFrameSerializer.Serialize(SyncMessageType.MerkleRootProbe, correlationId, probe);
        var frame = SyncWireFrameSerializer.Deserialize(bytes);

        frame.Type.Should().Be(SyncMessageType.MerkleRootProbe);
        frame.CorrelationId.Should().Be(correlationId);
        frame.Message.Should().BeOfType<MerkleRootProbe>();

        var roundtripped = (MerkleRootProbe)frame.Message;
        roundtripped.ClusterId.Should().Be(probe.ClusterId);
        roundtripped.RootHash.Should().Be(probe.RootHash);
    }

    [Fact]
    public void MerkleRootResponse_Roundtrip_PreservesData()
    {
        var response = new MerkleRootResponse(TestClusterId, true, "1122334455667788990011223344556677889900112233445566778899001122");
        ulong correlationId = 101;

        var bytes = SyncWireFrameSerializer.Serialize(SyncMessageType.MerkleRootResponse, correlationId, response);
        var frame = SyncWireFrameSerializer.Deserialize(bytes);

        frame.Type.Should().Be(SyncMessageType.MerkleRootResponse);
        frame.CorrelationId.Should().Be(correlationId);
        var roundtripped = (MerkleRootResponse)frame.Message;
        roundtripped.ClusterId.Should().Be(response.ClusterId);
        roundtripped.Matches.Should().BeTrue();
        roundtripped.RootHash.Should().Be(response.RootHash);
    }

    [Fact]
    public void PrefixDiffRequest_And_Response_Roundtrip_PreservesHierarchy()
    {
        var clock = VectorClock.Empty.Tick("peer-1").Tick("peer-2");
        var files = new List<WireFileRecord>
        {
            new("docs/readme.md", 1024, "aaaa", DateTimeOffset.UtcNow, clock, false, 1)
        };
        var subdirs = new List<WireMerkleNodeRecord>
        {
            new("docs/arch", "bbbb", 5)
        };

        var diffResponse = new PrefixDiffResponse("docs", false, files, subdirs);
        ulong correlationId = 555;

        var bytes = SyncWireFrameSerializer.Serialize(SyncMessageType.PrefixDiffResponse, correlationId, diffResponse);
        var frame = SyncWireFrameSerializer.Deserialize(bytes);

        frame.Type.Should().Be(SyncMessageType.PrefixDiffResponse);
        var roundtripped = (PrefixDiffResponse)frame.Message;
        roundtripped.Prefix.Should().Be("docs");
        roundtripped.AreIdentical.Should().BeFalse();
        roundtripped.Files.Should().HaveCount(1);
        roundtripped.Files[0].RelativePath.Should().Be("docs/readme.md");
        roundtripped.Files[0].Clock["peer-1"].Should().Be(1);
        roundtripped.Subdirectories.Should().HaveCount(1);
        roundtripped.Subdirectories[0].Prefix.Should().Be("docs/arch");
    }

    [Fact]
    public void FileManifestQuery_And_Response_Roundtrip_PreservesChunksAndClock()
    {
        var clock = VectorClock.Empty.Tick("node-A");
        var chunkHash = RandomNumberGenerator.GetBytes(32);
        var chunkHex = Convert.ToHexString(chunkHash).ToLowerInvariant();
        var chunks = new List<WireChunkRecord>
        {
            new(0, 0, 65536, chunkHex),
            new(1, 65536, 32768, chunkHex)
        };

        var manifestResp = new FileManifestResponse("src/main.cs", "contenthash123", 98304, clock, chunks);
        ulong correlationId = 777;

        var bytes = SyncWireFrameSerializer.Serialize(SyncMessageType.FileManifestResponse, correlationId, manifestResp);
        var frame = SyncWireFrameSerializer.Deserialize(bytes);

        frame.Type.Should().Be(SyncMessageType.FileManifestResponse);
        var roundtripped = (FileManifestResponse)frame.Message;
        roundtripped.RelativePath.Should().Be("src/main.cs");
        roundtripped.TotalBytes.Should().Be(98304);
        roundtripped.Clock["node-A"].Should().Be(1);
        roundtripped.Chunks.Should().HaveCount(2);
        roundtripped.Chunks[1].Offset.Should().Be(65536);
        roundtripped.Chunks[1].Length.Should().Be(32768);
        roundtripped.Chunks[1].HashHex.Should().Be(chunkHex);
    }

    [Fact]
    public void ChunkPayloadResponse_PreservesBinaryPayloadBitForBit_ZeroBase64Overhead()
    {
        byte[] payload = new byte[65536];
        RandomNumberGenerator.Fill(payload);
        byte[] hash = SHA256.HashData(payload);
        string hashHex = Convert.ToHexString(hash).ToLowerInvariant();

        var response = new ChunkPayloadResponse(hashHex, payload);
        ulong correlationId = 999;

        var bytes = SyncWireFrameSerializer.Serialize(SyncMessageType.ChunkPayloadResponse, correlationId, response);
        // Header (9) + Hash (32) + Payload (65536) = 65577 bytes strictly
        bytes.Length.Should().Be(SyncWireFrameSerializer.HeaderSize + SyncWireFrameSerializer.HashBinarySize + payload.Length);

        var frame = SyncWireFrameSerializer.Deserialize(bytes);
        frame.Type.Should().Be(SyncMessageType.ChunkPayloadResponse);
        frame.CorrelationId.Should().Be(correlationId);

        var roundtripped = (ChunkPayloadResponse)frame.Message;
        roundtripped.ChunkHash.Should().Be(hashHex);
        roundtripped.Payload.ToArray().Should().Equal(payload);
    }

    [Fact]
    public void ChunkFetchRequest_SerializesToExactBinarySize()
    {
        byte[] hash = RandomNumberGenerator.GetBytes(32);
        string hashHex = Convert.ToHexString(hash).ToLowerInvariant();

        var request = new ChunkFetchRequest(hashHex);
        ulong correlationId = 1234;

        var bytes = SyncWireFrameSerializer.Serialize(SyncMessageType.ChunkFetchRequest, correlationId, request);
        bytes.Length.Should().Be(SyncWireFrameSerializer.HeaderSize + SyncWireFrameSerializer.HashBinarySize);

        var frame = SyncWireFrameSerializer.Deserialize(bytes);
        frame.Type.Should().Be(SyncMessageType.ChunkFetchRequest);
        frame.CorrelationId.Should().Be(correlationId);

        var roundtripped = (ChunkFetchRequest)frame.Message;
        roundtripped.ChunkHash.Should().Be(hashHex);
    }

    [Fact]
    public void TruncatedFrame_ThrowsInvalidDataException()
    {
        byte[] shortBuffer = new byte[8]; // less than 9 bytes header
        var act = () => SyncWireFrameSerializer.Deserialize(shortBuffer);
        act.Should().Throw<InvalidDataException>().WithMessage("*too short*");
    }

    [Fact]
    public void PathTraversal_InQuery_ThrowsArgumentException()
    {
        var maliciousQuery = new FileManifestQuery("../secrets.txt");
        var serialized = SyncWireFrameSerializer.Serialize(SyncMessageType.FileManifestQuery, 1, maliciousQuery);

        var act = () => SyncWireFrameSerializer.Deserialize(serialized);
        act.Should().Throw<ArgumentException>().WithMessage("*path traversal*");
    }
}
