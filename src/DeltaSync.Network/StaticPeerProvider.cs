namespace DeltaSync.Network;

using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

/// <summary>
/// Manages statically configured peer endpoints (--peer <host>:<port>) and calculates
/// reconnection delays using exponential backoff with Full Jitter per Jampeltz et al. (2015).
/// </summary>
public sealed class StaticPeerProvider
{
    /// <summary>
    /// Base delay in seconds (B = 1.0s).
    /// </summary>
    public const double DefaultBaseSeconds = 1.0;

    /// <summary>
    /// Maximum backoff ceiling in seconds (M = 30.0s).
    /// </summary>
    public const double DefaultMaxSeconds = 30.0;

    private readonly object _syncRoot = new();
    private readonly PeerRegistry _registry;
    private readonly List<string> _rawEndpoints = new();
    private readonly Dictionary<string, IPEndPoint> _staticEndpoints = new(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyList<string> ConfiguredEndpoints
    {
        get
        {
            lock (_syncRoot)
            {
                return _rawEndpoints.ToList();
            }
        }
    }

    public StaticPeerProvider(PeerRegistry registry, IEnumerable<string>? initialEndpoints = null)
    {
        ArgumentNullException.ThrowIfNull(registry);
        _registry = registry;

        if (initialEndpoints is not null)
        {
            foreach (var ep in initialEndpoints)
            {
                AddStaticPeer(ep);
            }
        }
    }

    /// <summary>
    /// Parses and adds a static peer endpoint, registering it in the PeerRegistry.
    /// </summary>
    public bool AddStaticPeer(string endpointString, [NotNullWhen(true)] out PeerRecord? peer, [NotNullWhen(false)] out string? error)
    {
        if (!TryParseEndpoint(endpointString, out var endPoint, out error))
        {
            peer = null;
            return false;
        }

        lock (_syncRoot)
        {
            string key = endpointString.Trim();
            if (!_rawEndpoints.Contains(key, StringComparer.OrdinalIgnoreCase))
            {
                _rawEndpoints.Add(key);
            }
            _staticEndpoints[key] = endPoint;
        }

        string peerId = $"static-{endPoint.Address}-{endPoint.Port}";
        _registry.RegisterOrUpdateStatic(peerId, endPoint, out peer);
        error = null;
        return true;
    }

    /// <summary>
    /// Convenience method to add a static peer, throwing on malformed input.
    /// </summary>
    public PeerRecord AddStaticPeer(string endpointString)
    {
        if (!AddStaticPeer(endpointString, out var peer, out var error))
        {
            throw new ArgumentException($"Invalid static peer endpoint '{endpointString}': {error}", nameof(endpointString));
        }
        return peer;
    }

    /// <summary>
    /// Computes the exponential backoff delay with Full Jitter:
    /// Delay(a) = Uniform(0, min(M, B * 2^a))
    /// where B = 1.0s, M = 30.0s, and a is consecutive failure count.
    /// </summary>
    public static TimeSpan ComputeBackoff(
        int attempt,
        Random? random = null,
        double baseSeconds = DefaultBaseSeconds,
        double maxSeconds = DefaultMaxSeconds)
    {
        if (attempt < 0) attempt = 0;
        if (baseSeconds <= 0) baseSeconds = DefaultBaseSeconds;
        if (maxSeconds <= 0) maxSeconds = DefaultMaxSeconds;

        // Bounded exponent to prevent Math.Pow double overflow
        int clampedAttempt = Math.Min(attempt, 30);
        double exponential = baseSeconds * Math.Pow(2.0, clampedAttempt);
        double cap = Math.Min(maxSeconds, exponential);

        var rng = random ?? Random.Shared;
        double delaySeconds = rng.NextDouble() * cap;

        return TimeSpan.FromSeconds(delaySeconds);
    }

    /// <summary>
    /// Gets the next retry delay for a static peer based on its consecutive failure count in the registry.
    /// </summary>
    public TimeSpan GetNextRetryDelay(string peerId, Random? random = null)
    {
        if (_registry.TryGetPeer(peerId, out var peer))
        {
            return ComputeBackoff(peer.ConsecutiveFailures, random);
        }

        return ComputeBackoff(0, random);
    }

    /// <summary>
    /// Parses a string formatted as host:port, IPv4:port, or [IPv6]:port into a resolved IPEndPoint.
    /// </summary>
    public static bool TryParseEndpoint(
        string input,
        [NotNullWhen(true)] out IPEndPoint? endPoint,
        [NotNullWhen(false)] out string? error)
    {
        endPoint = null;
        if (string.IsNullOrWhiteSpace(input))
        {
            error = "Endpoint string cannot be null or whitespace.";
            return false;
        }

        input = input.Trim();

        string hostPart;
        string portPart;

        // IPv6 bracketed format: [2001:db8::1]:5001
        if (input.StartsWith('['))
        {
            int closingBracket = input.IndexOf(']');
            if (closingBracket == -1)
            {
                error = "Missing closing bracket ']' in IPv6 endpoint.";
                return false;
            }

            hostPart = input.Substring(1, closingBracket - 1);
            int colonIndex = input.IndexOf(':', closingBracket);
            if (colonIndex == -1 || colonIndex == input.Length - 1)
            {
                error = "Missing port delimiter ':' after IPv6 address.";
                return false;
            }

            portPart = input.Substring(colonIndex + 1);
        }
        else
        {
            // IPv4 or hostname: 192.168.1.1:5001 or localhost:5001
            int lastColon = input.LastIndexOf(':');
            if (lastColon == -1)
            {
                error = "Missing port delimiter ':'. Expected <host>:<port>.";
                return false;
            }

            hostPart = input[..lastColon];
            portPart = input[(lastColon + 1)..];
        }

        if (string.IsNullOrWhiteSpace(hostPart))
        {
            error = "Host component cannot be empty.";
            return false;
        }

        if (!ushort.TryParse(portPart, out ushort port) || port == 0)
        {
            error = $"Invalid port '{portPart}'. Port must be an integer between 1 and 65535.";
            return false;
        }

        // Resolve IP address
        if (IPAddress.TryParse(hostPart, out var parsedIp))
        {
            endPoint = new IPEndPoint(parsedIp, port);
            error = null;
            return true;
        }

        // Hostname resolution (e.g. localhost)
        if (string.Equals(hostPart, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            endPoint = new IPEndPoint(IPAddress.Loopback, port);
            error = null;
            return true;
        }

        try
        {
            var addresses = Dns.GetHostAddresses(hostPart);
            var selectedIp = addresses.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork)
                             ?? addresses.FirstOrDefault();

            if (selectedIp is null)
            {
                error = $"No IP address could be resolved for host '{hostPart}'.";
                return false;
            }

            endPoint = new IPEndPoint(selectedIp, port);
            error = null;
            return true;
        }
        catch (SocketException ex)
        {
            error = $"DNS resolution failed for host '{hostPart}': {ex.Message}";
            return false;
        }
    }
}
