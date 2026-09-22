namespace DeltaSync.Network;

using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

/// <summary>
/// Transmits periodic UDP multicast beacons with Floyd-Jacobson phase randomization jitter (±20%)
/// to avoid periodic synchronization broadcast storms across local networks.
/// </summary>
public sealed class UdpBeaconAnnouncer : IAsyncDisposable, IDisposable
{
    private readonly BeaconOptions _options;
    private readonly byte[] _localPeerIdHash;
    private readonly Random _random;
    private readonly Socket _socket;
    private readonly IPEndPoint _destinationEndPoint;
    private readonly bool _isMulticast;
    private readonly List<IPAddress> _multicastInterfaceAddresses;

    private ulong _sequence;
    private CancellationTokenSource? _cts;
    private Task? _broadcastTask;
    private bool _disposed;

    public ulong CurrentSequence => Interlocked.Read(ref _sequence);

    public UdpBeaconAnnouncer(BeaconOptions options, Random? random = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrEmpty(options.PeerId))
            throw new ArgumentException("PeerId must not be empty.", nameof(options));

        _options = options;
        _localPeerIdHash = BeaconFrame.ComputePeerIdHash(options.PeerId);
        _random = random ?? Random.Shared;
        _destinationEndPoint = new IPEndPoint(options.MulticastAddress, options.MulticastPort);
        _isMulticast = IsIPv4Multicast(options.MulticastAddress);

        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

        if (_isMulticast)
        {
            try
            {
                _socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 2);
                _socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastLoopback, true);
            }
            catch (SocketException)
            {
                // Ignore socket option failures in restricted testing environments
            }
        }

        _multicastInterfaceAddresses = _isMulticast ? EnumerateLocalMulticastInterfaces() : [];
    }

    /// <summary>
    /// Computes the next interval perturbed by uniform random jitter:
    /// T_next = T_base * (1 + rho), where rho ~ U(-jitterRatio, +jitterRatio).
    /// With T_base = 3.0s and jitterRatio = 0.20, T_next in [2.4s, 3.6s] (Floyd &amp; Jacobson 1993).
    /// </summary>
    public static TimeSpan ComputeJitteredInterval(TimeSpan baseInterval, double jitterRatio, Random? random = null)
    {
        random ??= Random.Shared;
        double rho = (random.NextDouble() * 2.0 - 1.0) * jitterRatio;
        double multiplier = 1.0 + rho;
        return TimeSpan.FromMilliseconds(baseInterval.TotalMilliseconds * multiplier);
    }

    /// <summary>
    /// Transmits a single beacon datagram immediately.
    /// </summary>
    public async ValueTask BroadcastOnceAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        ulong seq = Interlocked.Increment(ref _sequence);
        long timestamp = _options.TimeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        var frame = new BeaconFrame(
            BeaconFrame.ExpectedMagic,
            BeaconFrame.CurrentVersion,
            _options.ClusterId,
            _localPeerIdHash,
            _options.ListenPort,
            seq,
            timestamp
        );

        byte[] payload = frame.ToByteArray();

        if (_isMulticast && _multicastInterfaceAddresses.Count > 0)
        {
            foreach (var localAddr in _multicastInterfaceAddresses)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    _socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastInterface, localAddr.GetAddressBytes());
                    await _socket.SendToAsync(payload, SocketFlags.None, _destinationEndPoint, ct);
                }
                catch (SocketException)
                {
                    // Fall back to default send if interface-specific multicast fails
                    await _socket.SendToAsync(payload, SocketFlags.None, _destinationEndPoint, ct);
                }
            }
        }
        else
        {
            await _socket.SendToAsync(payload, SocketFlags.None, _destinationEndPoint, ct);
        }
    }

    /// <summary>
    /// Starts periodic beacon broadcasting in the background.
    /// </summary>
    public void Start(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_broadcastTask is not null) return;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _broadcastTask = Task.Run(() => RunBroadcastLoopAsync(_cts.Token));
    }

    private async Task RunBroadcastLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await BroadcastOnceAsync(ct);
                var delay = ComputeJitteredInterval(_options.BaseInterval, _options.JitterRatio, _random);
                await Task.Delay(delay, _options.TimeProvider, ct);
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown
        }
    }

    public async Task StopAsync()
    {
        if (_cts is not null)
        {
            _cts.Cancel();
            if (_broadcastTask is not null)
            {
                try
                {
                    await _broadcastTask;
                }
                catch (OperationCanceledException) { }
            }
            _cts.Dispose();
            _cts = null;
            _broadcastTask = null;
        }
    }

    private static bool IsIPv4Multicast(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork) return false;
        var bytes = address.GetAddressBytes();
        return bytes[0] >= 224 && bytes[0] <= 239;
    }

    private static List<IPAddress> EnumerateLocalMulticastInterfaces()
    {
        var result = new List<IPAddress>();
        try
        {
            foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (ni.OperationalStatus != OperationalStatus.Up) continue;
                if (!ni.SupportsMulticast) continue;
                if (ni.NetworkInterfaceType == NetworkInterfaceType.Loopback) continue;

                var ipProps = ni.GetIPProperties();
                foreach (var ua in ipProps.UnicastAddresses)
                {
                    if (ua.Address.AddressFamily == AddressFamily.InterNetwork)
                    {
                        result.Add(ua.Address);
                    }
                }
            }
        }
        catch
        {
            // Ignore in restricted environments
        }

        if (result.Count == 0)
        {
            result.Add(IPAddress.Any);
        }
        return result;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts?.Cancel();
        _cts?.Dispose();
        _socket.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        await StopAsync();
        Dispose();
    }
}
