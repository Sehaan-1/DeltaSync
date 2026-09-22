namespace DeltaSync.Network;

/// <summary>
/// Unified peer discovery engine combining UDP multicast beacons, static address configuration,
/// peer lifecycle registry, and liveness monitoring (Spec §3 & ADR-0002).
/// </summary>
public sealed class PeerDiscoveryEngine : IPeerDiscovery
{
    private readonly BeaconOptions _options;
    private readonly PeerRegistry _registry;
    private readonly UdpBeaconListener _listener;
    private readonly UdpBeaconAnnouncer _announcer;
    private readonly PeerLivenessTracker _livenessTracker;
    private readonly StaticPeerProvider _staticProvider;

    private bool _started;
    private bool _disposed;

    public event EventHandler<PeerRecord>? PeerDiscovered;
    public event EventHandler<PeerRecord>? PeerConnected;
    public event EventHandler<PeerRecord>? PeerStale;
    public event EventHandler<PeerRecord>? PeerLost;

    public IReadOnlyCollection<PeerRecord> ActivePeers => _registry.GetActivePeers();
    public PeerRegistry Registry => _registry;
    public StaticPeerProvider StaticProvider => _staticProvider;
    public UdpBeaconListener Listener => _listener;
    public UdpBeaconAnnouncer Announcer => _announcer;
    public PeerLivenessTracker LivenessTracker => _livenessTracker;

    public PeerDiscoveryEngine(
        BeaconOptions options,
        IEnumerable<string>? staticEndpoints = null,
        PeerLivenessOptions? livenessOptions = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        _options = options;
        var clock = timeProvider ?? TimeProvider.System;
        _registry = new PeerRegistry(clock);
        _listener = new UdpBeaconListener(options);
        _announcer = new UdpBeaconAnnouncer(options);
        _livenessTracker = new PeerLivenessTracker(_registry, livenessOptions, clock);
        _staticProvider = new StaticPeerProvider(_registry, staticEndpoints);

        WireEvents();
    }

    /// <summary>
    /// Internal constructor allowing explicit dependency injection for simulation testing.
    /// </summary>
    internal PeerDiscoveryEngine(
        BeaconOptions options,
        PeerRegistry registry,
        UdpBeaconListener listener,
        UdpBeaconAnnouncer announcer,
        PeerLivenessTracker livenessTracker,
        StaticPeerProvider staticProvider)
    {
        _options = options;
        _registry = registry;
        _listener = listener;
        _announcer = announcer;
        _livenessTracker = livenessTracker;
        _staticProvider = staticProvider;

        WireEvents();
    }

    private void WireEvents()
    {
        _listener.BeaconReceived += OnBeaconReceived;
        _registry.PeerDiscovered += (_, p) => PeerDiscovered?.Invoke(this, p);
        _registry.PeerConnected += (_, p) => PeerConnected?.Invoke(this, p);
        _registry.PeerStale += (_, p) => PeerStale?.Invoke(this, p);
        _registry.PeerLost += (_, p) => PeerLost?.Invoke(this, p);
    }

    private void OnBeaconReceived(object? sender, DiscoveredBeacon beacon)
    {
        _registry.RegisterOrUpdateBeacon(beacon, null, out _);
    }

    public Task StartAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_started) return Task.CompletedTask;
        _started = true;

        _listener.Start(ct);
        _announcer.Start(ct);
        _livenessTracker.Start();

        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (!_started || _disposed) return;
        _started = false;

        await _announcer.StopAsync();
        await _listener.StopAsync();
        await _livenessTracker.StopAsync();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _listener.BeaconReceived -= OnBeaconReceived;
        _announcer.Dispose();
        _listener.Dispose();
        _livenessTracker.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        Dispose();
    }
}
