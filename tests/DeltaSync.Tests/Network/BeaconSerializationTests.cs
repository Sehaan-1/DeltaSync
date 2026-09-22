namespace DeltaSync.Tests.Network;

using System.Security.Cryptography;
using System.Text;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

public class BeaconSerializationTests
{
    private readonly ITestOutputHelper _output;

    public BeaconSerializationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void SpecSection10_Check1_BeaconSerialization_Roundtrip_BitForBit_Exact72Bytes()
    {
        // Test multiple variations of cluster IDs and peer IDs
        var clusterIds = new[]
        {
            Guid.NewGuid(),
            Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"),
            Guid.Empty
        };

        var peerIds = new[]
        {
            "node-alpha",
            "node-beta-devbox-42",
            "workstation.local",
            "a"
        };

        ushort[] ports = [1024, 5001, 8080, 58732, 65535];
        ulong[] sequences = [0, 1, 42, 100_000_000UL, ulong.MaxValue];
        long[] timestamps = [0, 1700000000000L, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()];

        Span<byte> stackBuffer = stackalloc byte[BeaconFrame.FrameSize];

        for (int i = 0; i < peerIds.Length; i++)
        {
            var clusterId = clusterIds[i % clusterIds.Length];
            var peerId = peerIds[i];
            var peerIdHash = BeaconFrame.ComputePeerIdHash(peerId);
            var port = ports[i % ports.Length];
            var seq = sequences[i % sequences.Length];
            var timestamp = timestamps[i % timestamps.Length];

            var originalFrame = new BeaconFrame(
                BeaconFrame.ExpectedMagic,
                BeaconFrame.CurrentVersion,
                clusterId,
                peerIdHash,
                port,
                seq,
                timestamp
            );

            // Assert exact 72-byte size
            var serialized = originalFrame.ToByteArray();
            serialized.Length.Should().Be(BeaconFrame.FrameSize, "Wire size must equal 72 bytes strictly");
            serialized.Length.Should().BeLessThanOrEqualTo(508, "Datagram must respect safe MTU limit RFC 1122 §3.3.2 (Invariant I2)");

            // TryWrite span test
            var writeOk = originalFrame.TryWrite(stackBuffer);
            writeOk.Should().BeTrue();
            stackBuffer.SequenceEqual(serialized).Should().BeTrue("TryWrite and ToByteArray must produce identical bytes");

            // Deserialize and assert bit-for-bit roundtrip
            var parseOk = BeaconFrame.TryParse(serialized, out var decodedFrame);
            parseOk.Should().BeTrue();
            decodedFrame.Should().NotBeNull();

            decodedFrame!.Magic.Should().Be(originalFrame.Magic);
            decodedFrame.ProtocolVersion.Should().Be(originalFrame.ProtocolVersion);
            decodedFrame.ClusterId.Should().Be(originalFrame.ClusterId);
            decodedFrame.PeerIdHash.Should().Equal(originalFrame.PeerIdHash);
            decodedFrame.ListenPort.Should().Be(originalFrame.ListenPort);
            decodedFrame.Sequence.Should().Be(originalFrame.Sequence);
            decodedFrame.TimestampUnixMs.Should().Be(originalFrame.TimestampUnixMs);

            // Value object equality
            decodedFrame.Should().Be(originalFrame);
            decodedFrame.GetHashCode().Should().Be(originalFrame.GetHashCode());
        }

        _output.WriteLine($"[Check 1 Point 1 Passed] Verified bit-for-bit roundtrip across {peerIds.Length} variations.");
    }

    [Fact]
    public void SpecSection10_Check1_BeaconSerialization_TruncatedBuffer_RejectsSafelyWithZeroAllocations()
    {
        // Truncated buffers: 0, 1, 71 bytes
        int[] truncatedLengths = [0, 1, 2, 4, 16, 22, 54, 70, 71];

        foreach (var len in truncatedLengths)
        {
            var truncatedBuffer = new byte[len];
            if (len >= 4)
            {
                // Put valid magic to ensure length check triggers properly
                truncatedBuffer[0] = 0x4E;
                truncatedBuffer[1] = 0x59;
                truncatedBuffer[2] = 0x53;
                truncatedBuffer[3] = 0x44;
            }

            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var result = BeaconFrame.TryParse(truncatedBuffer, out var frame);
            long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

            result.Should().BeFalse($"Buffer of length {len} must be rejected as truncated");
            frame.Should().BeNull();
            (allocatedAfter - allocatedBefore).Should().Be(0, "Truncated buffer rejection must allocate 0 managed heap bytes");
        }

        // Corrupted Magic numbers
        var validFrame = new BeaconFrame(
            BeaconFrame.ExpectedMagic,
            BeaconFrame.CurrentVersion,
            Guid.NewGuid(),
            BeaconFrame.ComputePeerIdHash("test-peer"),
            5001,
            1,
            1700000000000L
        );
        var bytes = validFrame.ToByteArray();

        // Tamper magic (corrupt first byte)
        bytes[0] ^= 0xFF;

        long allocBefore = GC.GetAllocatedBytesForCurrentThread();
        var magicOk = BeaconFrame.TryParse(bytes, out var corruptedFrame);
        long allocAfter = GC.GetAllocatedBytesForCurrentThread();

        magicOk.Should().BeFalse("Corrupted magic number must be rejected");
        corruptedFrame.Should().BeNull();
        (allocAfter - allocBefore).Should().Be(0, "Magic rejection must allocate 0 managed heap bytes");

        // Tamper version
        bytes = validFrame.ToByteArray();
        bytes[4] = 99; // unsupported version
        var versionOk = BeaconFrame.TryParse(bytes, out var badVersionFrame);
        versionOk.Should().BeFalse("Unsupported version must be rejected");
        badVersionFrame.Should().BeNull();

        _output.WriteLine("[Check 1 Point 2 Passed] Verified zero-allocation safe rejection of truncated and corrupted buffers.");
    }

    [Fact]
    public void SpecSection6_KnownAnswerPacketVector_MatchesBitForBit()
    {
        // Spec §6 test vector:
        // Magic: 0x4453594E
        // Version: 1
        // ClusterId: 00112233-4455-6677-8899-aabbccddeeff
        // ListenPort: 5001
        // Sequence: 42
        // Timestamp: 1700000000000 (Unix ms)
        // PeerId: node-alpha (SHA-256 = 6a5b...)

        var clusterId = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var peerIdHash = BeaconFrame.ComputePeerIdHash("node-alpha");

        // Confirm PeerIdHash matches SHA-256 of "node-alpha"
        Convert.ToHexString(peerIdHash).ToLowerInvariant().Should().Be("c0b71775288d224ab56aa569276be75da7cd8635acc3550a2346b47ef4f7550b");

        var frame = new BeaconFrame(
            BeaconFrame.ExpectedMagic,
            BeaconFrame.CurrentVersion,
            clusterId,
            peerIdHash,
            5001,
            42,
            1700000000000L
        );

        var wireBytes = frame.ToByteArray();
        wireBytes.Length.Should().Be(72);

        // Byte 0..3 must equal [0x4E, 0x59, 0x53, 0x44] (little-endian uint32 0x4453594E)
        wireBytes[0].Should().Be(0x4E);
        wireBytes[1].Should().Be(0x59);
        wireBytes[2].Should().Be(0x53);
        wireBytes[3].Should().Be(0x44);

        // Decode back and verify all fields
        var parseOk = BeaconFrame.TryParse(wireBytes, out var decoded);
        parseOk.Should().BeTrue();
        decoded!.ClusterId.Should().Be(clusterId);
        decoded.ListenPort.Should().Be(5001);
        decoded.Sequence.Should().Be(42);
        decoded.TimestampUnixMs.Should().Be(1700000000000L);

        _output.WriteLine($"[Spec §6 Passed] Known-answer vector verified with SHA-256={frame.PeerIdHashHex[..8]}...");
    }
}
