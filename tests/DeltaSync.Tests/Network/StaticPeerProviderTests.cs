namespace DeltaSync.Tests.Network;

using System.Net;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

public class StaticPeerProviderTests
{
    private readonly ITestOutputHelper _output;

    public StaticPeerProviderTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Theory]
    [InlineData("192.168.1.50:5001", "192.168.1.50", 5001)]
    [InlineData("127.0.0.1:8080", "127.0.0.1", 8080)]
    [InlineData("localhost:9000", "127.0.0.1", 9000)]
    [InlineData("[::1]:5002", "::1", 5002)]
    [InlineData("[2001:db8::1]:65535", "2001:db8::1", 65535)]
    public void TryParseEndpoint_ValidFormats_SucceedsWithExactIPAndPort(string input, string expectedIp, int expectedPort)
    {
        bool ok = StaticPeerProvider.TryParseEndpoint(input, out var endPoint, out var error);

        ok.Should().BeTrue(because: error);
        endPoint.Should().NotBeNull();
        endPoint!.Address.ToString().Should().Be(expectedIp);
        endPoint.Port.Should().Be(expectedPort);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("192.168.1.1")]
    [InlineData("192.168.1.1:")]
    [InlineData("192.168.1.1:0")]
    [InlineData("192.168.1.1:70000")]
    [InlineData("192.168.1.1:-1")]
    [InlineData("192.168.1.1:abcd")]
    [InlineData("[::1:5000")]
    [InlineData("[:5000")]
    public void TryParseEndpoint_MalformedInputs_ReturnsFalseWithDescriptiveError(string invalidInput)
    {
        bool ok = StaticPeerProvider.TryParseEndpoint(invalidInput, out var endPoint, out var error);

        ok.Should().BeFalse();
        endPoint.Should().BeNull();
        error.Should().NotBeNullOrWhiteSpace();
    }

    [Fact]
    public void SpecSection10_Check3_StaticPeer_FullJitterBackoff_RespectsBoundsAndVariance()
    {
        // Spec §10 Check 3: Simulate consecutive connection failures across attempts 0..10;
        // assert retry delay is strictly bounded in [0, min(30, 2^attempt)] and exhibits expected variance
        // over 1,000 runs without thundering herd synchronization.
        const int samplesPerAttempt = 1000;
        var rng = new Random(42); // Seeded for repeatability

        for (int attempt = 0; attempt <= 10; attempt++)
        {
            double expectedCap = Math.Min(30.0, 1.0 * Math.Pow(2.0, attempt));
            var delays = new double[samplesPerAttempt];

            for (int i = 0; i < samplesPerAttempt; i++)
            {
                var delay = StaticPeerProvider.ComputeBackoff(attempt, rng);
                double seconds = delay.TotalSeconds;

                // Strict invariant: delay must be within [0, expectedCap]
                seconds.Should().BeInRange(0.0, expectedCap,
                    $"attempt {attempt} delay must stay within [0, {expectedCap:F1}s]");

                delays[i] = seconds;
            }

            double mean = delays.Average();
            double expectedMean = expectedCap / 2.0;

            // Theoretical variance for Uniform(0, c) is c^2 / 12, so stddev = c / sqrt(12)
            double variance = delays.Select(d => Math.Pow(d - mean, 2)).Average();
            double stddev = Math.Sqrt(variance);
            double expectedStdDev = expectedCap / Math.Sqrt(12.0);

            _output.WriteLine($"Attempt {attempt,2}: Cap={expectedCap,5:F1}s | Mean={mean,5:F2}s (exp: {expectedMean,5:F2}s) | StdDev={stddev,5:F2}s (exp: {expectedStdDev,5:F2}s)");

            // Mean should be within ±15% of expected uniform mean (except tiny caps where variance is fine)
            mean.Should().BeApproximately(expectedMean, Math.Max(0.2, expectedMean * 0.15));

            // Standard deviation should be within ±20% of expected uniform stddev
            stddev.Should().BeApproximately(expectedStdDev, Math.Max(0.2, expectedStdDev * 0.20));

            // Thundering herd check: min and max must span significant portion of the range
            double min = delays.Min();
            double max = delays.Max();
            (max - min).Should().BeGreaterThan(expectedCap * 0.85,
                $"samples must disperse across the interval without clustering");
        }
    }

    [Fact]
    public void StaticPeerProvider_RegistryIntegration_UpdatesConsecutiveFailuresAndBackoff()
    {
        var registry = new PeerRegistry();
        var provider = new StaticPeerProvider(registry);

        bool added = provider.AddStaticPeer("192.168.1.200:5001", out var peer, out var error);
        added.Should().BeTrue(because: error);
        peer.Should().NotBeNull();
        peer!.IsStatic.Should().BeTrue();

        // Initial failure count is 0 -> backoff bounded by 1.0s
        var delay0 = provider.GetNextRetryDelay(peer.PeerId);
        delay0.TotalSeconds.Should().BeInRange(0.0, 1.0);

        // Record 3 consecutive failures -> attempt 3 cap is 8.0s
        registry.RecordFailure(peer.PeerId);
        registry.RecordFailure(peer.PeerId);
        registry.RecordFailure(peer.PeerId);

        var delay3 = provider.GetNextRetryDelay(peer.PeerId);
        delay3.TotalSeconds.Should().BeInRange(0.0, 8.0);

        // Record success -> resets failure count
        registry.MarkConnected(peer.PeerId, out _);
        var delayReset = provider.GetNextRetryDelay(peer.PeerId);
        delayReset.TotalSeconds.Should().BeInRange(0.0, 1.0);
    }
}
