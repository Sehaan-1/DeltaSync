namespace DeltaSync.Tests.Network;

using DeltaSync.Network;
using FluentAssertions;
using Xunit;

public class HandshakeSerializationTests
{
    [Fact]
    public void HandshakeRequest_Serialization_Roundtrip_BitForBit()
    {
        var clusterId = Guid.NewGuid();
        var request = new HandshakeRequest(clusterId, "node-beta-devbox", 1, 5001);

        byte[] serialized = request.ToByteArray();
        serialized.Length.Should().Be(request.WireSize);

        bool parsed = HandshakeRequest.TryParse(serialized, out var decoded, out string? error);
        parsed.Should().BeTrue(error);
        decoded.Should().NotBeNull();
        decoded!.ClusterId.Should().Be(clusterId);
        decoded.PeerId.Should().Be("node-beta-devbox");
        decoded.ProtocolVersion.Should().Be(1);
        decoded.ListenPort.Should().Be(5001);
    }

    [Fact]
    public void HandshakeRequest_Truncated_ReturnsFalseWithoutThrowing()
    {
        var request = new HandshakeRequest(Guid.NewGuid(), "node-alpha", 1, 5001);
        byte[] serialized = request.ToByteArray();

        // Feed varying truncated lengths
        for (int len = 0; len < serialized.Length; len++)
        {
            byte[] truncated = serialized[..len];
            bool parsed = HandshakeRequest.TryParse(truncated, out var decoded, out string? error);
            parsed.Should().BeFalse();
            decoded.Should().BeNull();
            error.Should().NotBeNullOrWhiteSpace();
        }
    }

    [Fact]
    public void HandshakeRequest_InvalidMagic_ReturnsFalse()
    {
        var request = new HandshakeRequest(Guid.NewGuid(), "node-alpha", 1, 5001);
        byte[] serialized = request.ToByteArray();
        serialized[0] ^= 0xFF; // Corrupt magic

        bool parsed = HandshakeRequest.TryParse(serialized, out var decoded, out string? error);
        parsed.Should().BeFalse();
        decoded.Should().BeNull();
        error.Should().Contain("Invalid handshake magic");
    }

    [Fact]
    public void HandshakeRequest_OversizedPeerId_FailsValidation()
    {
        string oversizedPeerId = new('x', HandshakeRequest.MaxPeerIdByteLength + 1);
        var request = new HandshakeRequest(Guid.NewGuid(), oversizedPeerId, 1, 5001);

        request.IsValid(out string? error).Should().BeFalse();
        error.Should().Contain("exceeds maximum allowed");
    }

    [Fact]
    public void HandshakeResponse_Success_Roundtrip()
    {
        var clusterId = Guid.NewGuid();
        var response = HandshakeResponse.CreateSuccess(clusterId, "node-beta");

        byte[] serialized = response.ToByteArray();
        bool parsed = HandshakeResponse.TryParse(serialized, out var decoded, out string? error);
        parsed.Should().BeTrue(error);
        decoded.Should().NotBeNull();
        decoded!.Status.Should().Be(HandshakeStatus.Success);
        decoded.IsSuccess.Should().BeTrue();
        decoded.ClusterId.Should().Be(clusterId);
        decoded.PeerId.Should().Be("node-beta");
        decoded.ErrorMessage.Should().BeNull();
    }

    [Theory]
    [InlineData(HandshakeStatus.ClusterMismatch)]
    [InlineData(HandshakeStatus.VersionIncompatible)]
    [InlineData(HandshakeStatus.CollisionRejected)]
    [InlineData(HandshakeStatus.DuplicateConnection)]
    [InlineData(HandshakeStatus.MalformedRequest)]
    public void HandshakeResponse_ErrorStatuses_Roundtrip(HandshakeStatus status)
    {
        var response = new HandshakeResponse(status, $"Error details for {status}", Guid.NewGuid(), "node-local");
        byte[] serialized = response.ToByteArray();

        bool parsed = HandshakeResponse.TryParse(serialized, out var decoded, out string? error);
        parsed.Should().BeTrue(error);
        decoded.Should().NotBeNull();
        decoded!.Status.Should().Be(status);
        decoded.IsSuccess.Should().BeFalse();
        decoded.ErrorMessage.Should().Be($"Error details for {status}");
        decoded.PeerId.Should().Be("node-local");
    }
}
