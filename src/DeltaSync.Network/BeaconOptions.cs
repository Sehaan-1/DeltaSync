namespace DeltaSync.Network;

using System.Net;

/// <summary>
/// Configuration options for UDP multicast peer beacon announcer and listener.
/// </summary>
public sealed record BeaconOptions
{
    /// <summary>
    /// Default IPv4 multicast discovery group address (ADR-0002).
    /// </summary>
    public const string DefaultMulticastAddress = "239.255.42.99";

    /// <summary>
    /// Default UDP multicast discovery port.
    /// </summary>
    public const int DefaultMulticastPort = 58732;

    /// <summary>
    /// Default Floyd-Jacobson phase randomization jitter ratio (±20%).
    /// </summary>
    public const double DefaultJitterRatio = 0.20;

    /// <summary>
    /// Default base broadcast interval (3.0 seconds).
    /// </summary>
    public static readonly TimeSpan DefaultBaseInterval = TimeSpan.FromSeconds(3.0);

    /// <summary>
    /// Multicast IP address used for beacon transmission and reception.
    /// </summary>
    public IPAddress MulticastAddress { get; init; } = IPAddress.Parse(DefaultMulticastAddress);

    /// <summary>
    /// UDP port used for beacon transmission and reception.
    /// </summary>
    public int MulticastPort { get; init; } = DefaultMulticastPort;

    /// <summary>
    /// Synchronization cluster GUID to discover. Beacons for differing clusters are dropped.
    /// </summary>
    public Guid ClusterId { get; init; }

    /// <summary>
    /// Canonical local peer identifier.
    /// </summary>
    public string PeerId { get; init; } = string.Empty;

    /// <summary>
    /// Listening TCP/gRPC server port advertised to discovered peers.
    /// </summary>
    public ushort ListenPort { get; init; }

    /// <summary>
    /// Base beacon broadcast interval before jitter perturbation.
    /// </summary>
    public TimeSpan BaseInterval { get; init; } = DefaultBaseInterval;

    /// <summary>
    /// Uniform jitter perturbation ratio (e.g. 0.20 for ±20%).
    /// </summary>
    public double JitterRatio { get; init; } = DefaultJitterRatio;

    /// <summary>
    /// Time provider for monotonic timestamps and test scheduling.
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
