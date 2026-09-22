namespace DeltaSync.Tests.Network;

using DeltaSync.Network;
using FluentAssertions;
using Xunit;

public class BgpCollisionTieBreakerTests
{
    [Fact]
    public void Resolve_WhenLocalHigher_PreservesOutbound()
    {
        var decision = BgpCollisionTieBreaker.Resolve("node-beta", "node-alpha");
        decision.Should().Be(CollisionDecision.PreserveOutbound);
        BgpCollisionTieBreaker.ShouldPreserveOutbound("node-beta", "node-alpha").Should().BeTrue();
        BgpCollisionTieBreaker.ShouldYieldToInbound("node-beta", "node-alpha").Should().BeFalse();
    }

    [Fact]
    public void Resolve_WhenLocalLower_YieldsToInbound()
    {
        var decision = BgpCollisionTieBreaker.Resolve("node-alpha", "node-beta");
        decision.Should().Be(CollisionDecision.YieldToInbound);
        BgpCollisionTieBreaker.ShouldPreserveOutbound("node-alpha", "node-beta").Should().BeFalse();
        BgpCollisionTieBreaker.ShouldYieldToInbound("node-alpha", "node-beta").Should().BeTrue();
    }

    [Theory]
    [InlineData("a", "b")]
    [InlineData("node-1", "node-2")]
    [InlineData("node-10", "node-2")] // '1' < '2' in ordinal
    [InlineData("alpha", "beta")]
    [InlineData("client-A", "client-B")]
    [InlineData("018e69d7-83c1-744b-97e3-4638da0ef052", "018e69d7-83c1-744b-97e3-4638da0ef053")]
    public void Resolve_IsStrictlySymmetricAndOpposite(string peerLow, string peerHigh)
    {
        // Assert peerLow is indeed lower than peerHigh
        string.CompareOrdinal(peerLow, peerHigh).Should().BeNegative();

        // At peerLow:
        BgpCollisionTieBreaker.Resolve(peerLow, peerHigh).Should().Be(CollisionDecision.YieldToInbound);

        // At peerHigh:
        BgpCollisionTieBreaker.Resolve(peerHigh, peerLow).Should().Be(CollisionDecision.PreserveOutbound);
    }

    [Fact]
    public void Resolve_SelfConnection_ThrowsInvalidOperationException()
    {
        var act = () => BgpCollisionTieBreaker.Resolve("node-alpha", "node-alpha");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*self-connection*");
    }

    [Theory]
    [InlineData(null, "remote")]
    [InlineData("", "remote")]
    [InlineData("   ", "remote")]
    [InlineData("local", null)]
    [InlineData("local", "")]
    [InlineData("local", "   ")]
    public void Resolve_NullOrEmptyId_ThrowsArgumentException(string? local, string? remote)
    {
        var act = () => BgpCollisionTieBreaker.Resolve(local!, remote!);
        act.Should().Throw<ArgumentException>();
    }
}
