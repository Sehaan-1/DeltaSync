namespace DeltaSync.Tests.Network;

using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;

public class TcpTransportChannelTests
{
    [Fact]
    public async Task RoundtripStreaming_500Frames_ExactFifoDelivery()
    {
        var (local, remote) = await TcpTransportChannel.CreateConnectedTcpPairAsync("node-A", "node-B");

        try
        {
            const int frameCount = 500;
            var sentFrames = new List<(byte[] Data, byte[] Hash)>(frameCount);
            var random = new Random(42);

            for (int i = 0; i < frameCount; i++)
            {
                // Frame sizes from 1 byte up to 64 KB (with some up to 256 KB)
                int size;
                if (i % 50 == 0)
                {
                    size = 256 * 1024; // 256 KB frame
                }
                else if (i % 10 == 0)
                {
                    size = 64 * 1024; // 64 KB frame
                }
                else
                {
                    size = random.Next(1, 4096);
                }

                byte[] data = new byte[size];
                random.NextBytes(data);
                byte[] hash = SHA256.HashData(data);
                sentFrames.Add((data, hash));
            }

            var receiveTask = Task.Run(async () =>
            {
                var received = new List<byte[]>(frameCount);
                for (int i = 0; i < frameCount; i++)
                {
                    var memory = await remote.ReceiveAsync();
                    memory.IsEmpty.Should().BeFalse();
                    received.Add(memory.ToArray());
                }
                return received;
            });

            foreach (var frame in sentFrames)
            {
                await local.SendAsync(frame.Data);
            }

            var receivedFrames = await receiveTask;
            receivedFrames.Count.Should().Be(frameCount);

            for (int i = 0; i < frameCount; i++)
            {
                receivedFrames[i].Length.Should().Be(sentFrames[i].Data.Length, $"Frame {i} length mismatch");
                byte[] actualHash = SHA256.HashData(receivedFrames[i]);
                actualHash.Should().Equal(sentFrames[i].Hash, $"Frame {i} content hash mismatch");
            }
        }
        finally
        {
            await local.DisposeAsync();
            await remote.DisposeAsync();
        }
    }

    [Fact]
    public async Task HeaderValidation_OversizedFrame_ThrowsAndClosesSocket()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        using var rawClient = new TcpClient();
        var connectTask = rawClient.ConnectAsync(IPAddress.Loopback, port);
        using var serverClient = await listener.AcceptTcpClientAsync();
        await connectTask;
        listener.Stop();

        var channel = new TcpTransportChannel(
            "server",
            "client",
            Guid.NewGuid(),
            serverClient.GetStream(),
            isInbound: true,
            socket: serverClient.Client);

        try
        {
            // Send 5 MB length header (5,242,880 bytes > 4 MB max)
            var rawStream = rawClient.GetStream();
            byte[] header = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(header, 5 * 1024 * 1024);
            await rawStream.WriteAsync(header);
            await rawStream.FlushAsync();

            // ReceiveAsync should throw InvalidDataException
            var act = async () => await channel.ReceiveAsync();
            await act.Should().ThrowAsync<InvalidDataException>()
                .WithMessage("*Invalid transport frame length*");

            channel.IsConnected.Should().BeFalse();
        }
        finally
        {
            await channel.DisposeAsync();
        }
    }

    [Fact]
    public async Task HeaderValidation_ZeroLengthFrame_ThrowsAndClosesSocket()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        using var rawClient = new TcpClient();
        var connectTask = rawClient.ConnectAsync(IPAddress.Loopback, port);
        using var serverClient = await listener.AcceptTcpClientAsync();
        await connectTask;
        listener.Stop();

        var channel = new TcpTransportChannel(
            "server",
            "client",
            Guid.NewGuid(),
            serverClient.GetStream(),
            isInbound: true,
            socket: serverClient.Client);

        try
        {
            // Send 0 length header (0 < 1 min)
            var rawStream = rawClient.GetStream();
            byte[] header = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(header, 0);
            await rawStream.WriteAsync(header);
            await rawStream.FlushAsync();

            var act = async () => await channel.ReceiveAsync();
            await act.Should().ThrowAsync<InvalidDataException>()
                .WithMessage("*Invalid transport frame length*");

            channel.IsConnected.Should().BeFalse();
        }
        finally
        {
            await channel.DisposeAsync();
        }
    }

    [Fact]
    public async Task HeaderValidation_MaxAllowedFrame_Succeeds()
    {
        var (local, remote) = await TcpTransportChannel.CreateConnectedTcpPairAsync("node-A", "node-B");

        try
        {
            // 4 MB frame (MaxFrameSize)
            byte[] largeFrame = new byte[TcpTransportChannel.MaxFrameSize];
            largeFrame[0] = 0xAA;
            largeFrame[^1] = 0xBB;

            var receiveTask = Task.Run(async () => await remote.ReceiveAsync());

            await local.SendAsync(largeFrame);

            var received = await receiveTask;
            received.Length.Should().Be(TcpTransportChannel.MaxFrameSize);
            received.Span[0].Should().Be(0xAA);
            received.Span[^1].Should().Be(0xBB);
        }
        finally
        {
            await local.DisposeAsync();
            await remote.DisposeAsync();
        }
    }

    [Fact]
    public async Task ConcurrentSendAsync_AtomicityPreserved()
    {
        var (local, remote) = await TcpTransportChannel.CreateConnectedTcpPairAsync("node-A", "node-B");

        try
        {
            const int concurrency = 10;
            const int framesPerTask = 50;
            const int totalFrames = concurrency * framesPerTask;

            var receiveTask = Task.Run(async () =>
            {
                var received = new List<byte[]>(totalFrames);
                for (int i = 0; i < totalFrames; i++)
                {
                    var mem = await remote.ReceiveAsync();
                    mem.IsEmpty.Should().BeFalse();
                    received.Add(mem.ToArray());
                }
                return received;
            });

            var sendTasks = Enumerable.Range(0, concurrency).Select(taskId => Task.Run(async () =>
            {
                for (int seq = 0; seq < framesPerTask; seq++)
                {
                    // Pattern: [taskId (4B), seq (4B), length (4B), repeated byte (taskId)]
                    int payloadLen = 100 + (seq * 10);
                    byte[] payload = new byte[payloadLen];
                    BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(0, 4), taskId);
                    BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(4, 4), seq);
                    BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(8, 4), payloadLen);
                    for (int j = 12; j < payloadLen; j++)
                    {
                        payload[j] = (byte)(taskId + 1);
                    }

                    await local.SendAsync(payload);
                }
            })).ToArray();

            await Task.WhenAll(sendTasks);
            var allReceived = await receiveTask;

            allReceived.Count.Should().Be(totalFrames);

            // Verify each frame is completely uncorrupted
            var perTaskCounts = new int[concurrency];
            foreach (var frame in allReceived)
            {
                frame.Length.Should().BeGreaterOrEqualTo(12);
                int taskId = BinaryPrimitives.ReadInt32BigEndian(frame.AsSpan(0, 4));
                int seq = BinaryPrimitives.ReadInt32BigEndian(frame.AsSpan(4, 4));
                int recordedLen = BinaryPrimitives.ReadInt32BigEndian(frame.AsSpan(8, 4));

                taskId.Should().BeInRange(0, concurrency - 1);
                recordedLen.Should().Be(frame.Length);

                for (int j = 12; j < frame.Length; j++)
                {
                    frame[j].Should().Be((byte)(taskId + 1), "Payload bytes must not be interleaved or corrupted");
                }

                perTaskCounts[taskId]++;
            }

            foreach (var count in perTaskCounts)
            {
                count.Should().Be(framesPerTask);
            }
        }
        finally
        {
            await local.DisposeAsync();
            await remote.DisposeAsync();
        }
    }

    [Fact]
    public async Task GracefulClose_SignalsEofAndReleasesResources()
    {
        var (local, remote) = await TcpTransportChannel.CreateConnectedTcpPairAsync("node-A", "node-B");

        try
        {
            byte[] msg = Encoding.UTF8.GetBytes("message before close");
            await local.SendAsync(msg);
            var received = await remote.ReceiveAsync();
            Encoding.UTF8.GetString(received.Span).Should().Be("message before close");

            await local.CloseAsync("normal closure");
            local.IsConnected.Should().BeFalse();

            var eofMemory = await remote.ReceiveAsync();
            eofMemory.IsEmpty.Should().BeTrue();
            remote.IsConnected.Should().BeFalse();
        }
        finally
        {
            await local.DisposeAsync();
            await remote.DisposeAsync();
        }
    }

    [Fact]
    public async Task Dispose_SetsIsDisposedAndPreventsSend()
    {
        var (local, remote) = await TcpTransportChannel.CreateConnectedTcpPairAsync("node-A", "node-B");

        try
        {
            local.Dispose();
            local.IsDisposed.Should().BeTrue();

            var act = async () => await local.SendAsync(new byte[] { 1, 2, 3 });
            await act.Should().ThrowAsync<ObjectDisposedException>();
        }
        finally
        {
            await local.DisposeAsync();
            await remote.DisposeAsync();
        }
    }
}
