namespace DeltaSync.Network;

using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

/// <summary>
/// Asynchronously listens for UDP multicast discovery beacons, enforcing safe datagram sizing (RFC 1122),
/// cluster isolation (P2), and multicast loopback suppression (I3).
/// </summary>
public sealed class UdpBeaconListener : IAsyncDisposable, IDisposable
{
    private readonly BeaconOptions _options;
    private readonly byte[] _localPeerIdHash;
    private readonly Socket _socket;
    private readonly bool _isMulticast;

    private long _packetsReceived;
    private long _selfBeaconsSuppressed;
    private long _droppedPackets;

    private CancellationTokenSource? _cts;
    private Task? _listenTask;
    private bool _disposed;

    /// <summary>
    /// Event raised when a valid discovery beacon from a remote peer sharing the same ClusterId arrives.
    /// </summary>
    public event EventHandler<DiscoveredBeacon>? BeaconReceived;

    /// <summary>
    /// Total count of UDP datagrams received at the socket boundary.
    /// </summary>
    public long PacketsReceived => Interlocked.Read(ref _packetsReceived);

    /// <summary>
    /// Total count of self-emitted beacons dropped by loopback suppression (Invariant I3).
    /// </summary>
    public long SelfBeaconsSuppressed => Interlocked.Read(ref _selfBeaconsSuppressed);

    /// <summary>
    /// Total count of malformed, truncated, or foreign cluster packets dropped (Postcondition P2).
    /// </summary>
    public long DroppedPackets => Interlocked.Read(ref _droppedPackets);

    public UdpBeaconListener(BeaconOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrEmpty(options.PeerId))
            throw new ArgumentException("PeerId must not be empty.", nameof(options));

        _options = options;
        _localPeerIdHash = BeaconFrame.ComputePeerIdHash(options.PeerId);
        _isMulticast = IsIPv4Multicast(options.MulticastAddress);

        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

        // Bind listener socket to receive datagrams on configured port
        _socket.Bind(new IPEndPoint(IPAddress.Any, options.MulticastPort));

        if (_isMulticast)
        {
            JoinMulticastGroup(options.MulticastAddress);
        }
    }

    /// <summary>
    /// Starts the background receive loop.
    /// </summary>
    public void Start(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_listenTask is not null) return;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _listenTask = Task.Run(() => RunReceiveLoopAsync(_cts.Token));
    }

    private async Task RunReceiveLoopAsync(CancellationToken ct)
    {
        // 512-byte buffer strictly handles max non-fragmented UDP datagrams (RFC 1122 <= 508 bytes)
        byte[] buffer = new byte[512];

        try
        {
            while (!ct.IsCancellationRequested)
            {
                EndPoint remoteEndPoint = new IPEndPoint(IPAddress.Any, 0);
                SocketReceiveFromResult result;

                try
                {
                    result = await _socket.ReceiveFromAsync(buffer, SocketFlags.None, remoteEndPoint, ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (SocketException ex) when (ex.SocketErrorCode == SocketError.OperationAborted ||
                                                ex.SocketErrorCode == SocketError.Interrupted)
                {
                    break;
                }

                Interlocked.Increment(ref _packetsReceived);
                ProcessReceivedPacket(buffer, result.ReceivedBytes, result.RemoteEndPoint);
            }
        }
        catch (Exception) when (_disposed || ct.IsCancellationRequested)
        {
            // Graceful exit on shutdown
        }
    }

    private void ProcessReceivedPacket(byte[] buffer, int receivedBytes, EndPoint remoteEndPoint)
    {
        ReadOnlySpan<byte> receivedData = buffer.AsSpan(0, receivedBytes);

        // 1. Attempt zero-allocation parse
        if (!BeaconFrame.TryParse(receivedData, out var frame))
        {
            Interlocked.Increment(ref _droppedPackets);
            return;
        }

        // 2. Postcondition P2: Cluster isolation (ignore beacons for other folders/clusters)
        if (frame.ClusterId != _options.ClusterId)
        {
            Interlocked.Increment(ref _droppedPackets);
            return;
        }

        // 3. Invariant I3: Loopback suppression (filter self-beacons)
        if (frame.PeerIdHash.AsSpan().SequenceEqual(_localPeerIdHash))
        {
            Interlocked.Increment(ref _selfBeaconsSuppressed);
            return;
        }

        // 4. Emit validated discovery event
        var senderEndPoint = (IPEndPoint)remoteEndPoint;
        var discovered = new DiscoveredBeacon(frame, senderEndPoint);
        BeaconReceived?.Invoke(this, discovered);
    }

    public async Task StopAsync()
    {
        if (_cts is not null)
        {
            _cts.Cancel();
            try
            {
                _socket.Close();
            }
            catch
            {
                // Ignore socket close errors
            }

            if (_listenTask is not null)
            {
                try
                {
                    await _listenTask;
                }
                catch (OperationCanceledException) { }
            }

            _cts.Dispose();
            _cts = null;
            _listenTask = null;
        }
    }

    private void JoinMulticastGroup(IPAddress multicastAddress)
    {
        try
        {
            // Join on default interface
            var defaultMcast = new MulticastOption(multicastAddress, IPAddress.Any);
            _socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership, defaultMcast);
        }
        catch (SocketException)
        {
            // Fall back or continue
        }

        // Join on all active multicast network interfaces
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
                        try
                        {
                            var mcastOpt = new MulticastOption(multicastAddress, ua.Address);
                            _socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership, mcastOpt);
                        }
                        catch (SocketException)
                        {
                            // Interface may already be joined or restricted
                        }
                    }
                }
            }
        }
        catch
        {
            // Ignore in restricted environments
        }
    }

    private static bool IsIPv4Multicast(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork) return false;
        var bytes = address.GetAddressBytes();
        return bytes[0] >= 224 && bytes[0] <= 239;
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
