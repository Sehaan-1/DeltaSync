namespace DeltaSync.Tests.Network;

using System.Text;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;

public class InMemoryTransportChannelTests
{
    [Fact]
    public async Task CreateConnectedPair_SendsAndReceivesBidirectionally()
    {
        var clusterId = Guid.NewGuid();
        var (nodeA, nodeB) = InMemoryTransportChannel.CreateConnectedPair("node-A", "node-B", clusterId);

        nodeA.LocalPeerId.Should().Be("node-A");
        nodeA.RemotePeerId.Should().Be("node-B");
        nodeB.LocalPeerId.Should().Be("node-B");
        nodeB.RemotePeerId.Should().Be("node-A");

        // A -> B
        byte[] msgToB = Encoding.UTF8.GetBytes("hello from A");
        await nodeA.SendAsync(msgToB);
        var receivedByB = await nodeB.ReceiveAsync();
        Encoding.UTF8.GetString(receivedByB.Span).Should().Be("hello from A");

        // B -> A
        byte[] msgToA = Encoding.UTF8.GetBytes("hello from B");
        await nodeB.SendAsync(msgToA);
        var receivedByA = await nodeA.ReceiveAsync();
        Encoding.UTF8.GetString(receivedByA.Span).Should().Be("hello from B");

        await nodeA.DisposeAsync();
        await nodeB.DisposeAsync();
    }

    [Fact]
    public async Task CloseAsync_CompletesRemoteReaderWithEmptyMemory()
    {
        var clusterId = Guid.NewGuid();
        var (nodeA, nodeB) = InMemoryTransportChannel.CreateConnectedPair("node-A", "node-B", clusterId);

        await nodeA.CloseAsync("normal closure");
        nodeA.IsConnected.Should().BeFalse();

        var received = await nodeB.ReceiveAsync();
        received.IsEmpty.Should().BeTrue();

        await nodeA.DisposeAsync();
        await nodeB.DisposeAsync();
    }

    [Fact]
    public async Task Dispose_SetsIsDisposedAndPreventsSend()
    {
        var clusterId = Guid.NewGuid();
        var (nodeA, nodeB) = InMemoryTransportChannel.CreateConnectedPair("node-A", "node-B", clusterId);

        nodeA.Dispose();
        nodeA.IsDisposed.Should().BeTrue();

        var act = async () => await nodeA.SendAsync(new byte[] { 1, 2, 3 });
        await act.Should().ThrowAsync<ObjectDisposedException>();

        await nodeB.DisposeAsync();
    }
}
