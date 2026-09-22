using DeltaSync.Core.Causality;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Causality;

public class ConflictResolverTests
{
    [Fact]
    public void Branch1_EqualVectors_ReturnsNoOp()
    {
        var clockA = VectorClock.Create(("NodeA", 2), ("NodeB", 1));
        var clockB = VectorClock.Create(("NodeA", 2), ("NodeB", 1));

        var result = ConflictResolver.Resolve(
            localPeerId: "NodeA",
            relativePath: "doc.txt",
            localHash: "hash123",
            localClock: clockA,
            remotePeerId: "NodeB",
            remoteHash: "hash123",
            remoteClock: clockB);

        result.Type.Should().Be(ConflictResolutionType.NoOp);
        result.CausalRelation.Should().Be(CausalRelation.Equal);
        result.PrimaryPath.Should().Be("doc.txt");
        (result.PrimaryVector == clockA).Should().BeTrue();
        result.SiblingPath.Should().BeNull();
        result.SiblingVector.Should().BeNull();
    }

    [Fact]
    public void Branch2_RemoteDominatesLocal_ReturnsApplyRemote()
    {
        var clockA = VectorClock.Create(("NodeA", 1));
        var clockB = VectorClock.Create(("NodeA", 1), ("NodeB", 1));

        var result = ConflictResolver.Resolve(
            localPeerId: "NodeA",
            relativePath: "report.pdf",
            localHash: "hashOld",
            localClock: clockA,
            remotePeerId: "NodeB",
            remoteHash: "hashNew",
            remoteClock: clockB);

        result.Type.Should().Be(ConflictResolutionType.ApplyRemote);
        result.CausalRelation.Should().Be(CausalRelation.Before);
        result.PrimaryPath.Should().Be("report.pdf");
        (result.PrimaryVector == clockB).Should().BeTrue();
        result.SiblingPath.Should().BeNull();
        result.SiblingVector.Should().BeNull();
    }

    [Fact]
    public void Branch2_NewLocalFile_ReturnsApplyRemote()
    {
        var clockB = VectorClock.Create(("NodeB", 1));

        var remoteUpdate = new RemoteFileUpdate(
            RelativePath: "newfile.txt",
            PeerId: "NodeB",
            ContentHash: "hashB",
            Clock: clockB);

        var result = ConflictResolver.Resolve(
            localPeerId: "NodeA",
            localState: null,
            remoteUpdate: remoteUpdate);

        result.Type.Should().Be(ConflictResolutionType.ApplyRemote);
        result.CausalRelation.Should().Be(CausalRelation.Before);
        (result.PrimaryVector == clockB).Should().BeTrue();
        result.SiblingPath.Should().BeNull();
    }

    [Fact]
    public void Branch3_LocalDominatesRemote_ReturnsRejectObsolete()
    {
        var clockA = VectorClock.Create(("NodeA", 2), ("NodeB", 1));
        var clockB = VectorClock.Create(("NodeA", 1), ("NodeB", 1));

        var result = ConflictResolver.Resolve(
            localPeerId: "NodeA",
            relativePath: "data.csv",
            localHash: "currentHash",
            localClock: clockA,
            remotePeerId: "NodeB",
            remoteHash: "staleHash",
            remoteClock: clockB);

        result.Type.Should().Be(ConflictResolutionType.RejectObsolete);
        result.CausalRelation.Should().Be(CausalRelation.After);
        result.PrimaryPath.Should().Be("data.csv");
        (result.PrimaryVector == clockA).Should().BeTrue();
        result.SiblingPath.Should().BeNull();
        result.SiblingVector.Should().BeNull();
    }

    [Fact]
    public void Branch4a_ConcurrentIdenticalContent_MergesVectorsWithoutDuplicate()
    {
        var clockA = VectorClock.Create(("NodeA", 1));
        var clockB = VectorClock.Create(("NodeB", 1));

        var result = ConflictResolver.Resolve(
            localPeerId: "NodeA",
            relativePath: "shared.txt",
            localHash: "exact-same-content-hash",
            localClock: clockA,
            remotePeerId: "NodeB",
            remoteHash: "exact-same-content-hash",
            remoteClock: clockB);

        result.Type.Should().Be(ConflictResolutionType.MergeIdentical);
        result.CausalRelation.Should().Be(CausalRelation.Concurrent);
        result.PrimaryPath.Should().Be("shared.txt");
        (result.PrimaryVector == VectorClock.Create(("NodeA", 1), ("NodeB", 1))).Should().BeTrue();
        result.SiblingPath.Should().BeNull();
        result.SiblingVector.Should().BeNull();
    }

    [Fact]
    public void Branch4b_ConcurrentDivergentContent_PreservesSideBySideWithAttribution()
    {
        var clockA = VectorClock.Create(("NodeA", 1));
        var clockB = VectorClock.Create(("NodeB", 1));

        var result = ConflictResolver.Resolve(
            localPeerId: "NodeA",
            relativePath: "project/notes.txt",
            localHash: "local-hash-a",
            localClock: clockA,
            remotePeerId: "NodeB",
            remoteHash: "remote-hash-b",
            remoteClock: clockB);

        result.Type.Should().Be(ConflictResolutionType.PreserveSideBySide);
        result.CausalRelation.Should().Be(CausalRelation.Concurrent);
        result.PrimaryPath.Should().Be("project/notes.txt");
        result.SiblingPath.Should().Be("project/notes (NodeB conflicted).txt");
        (result.SiblingVector == clockB).Should().BeTrue();

        // Unified vector: Tick(VA ⊔ VB, NodeA) = Tick({NodeA: 1, NodeB: 1}, NodeA) = {NodeA: 2, NodeB: 1}
        var expectedUnified = VectorClock.Create(("NodeA", 2), ("NodeB", 1));
        (result.PrimaryVector == expectedUnified).Should().BeTrue();

        // Invariant: unified vector strictly dominates both predecessors
        result.PrimaryVector.IsDominating(clockA).Should().BeTrue();
        result.PrimaryVector.IsDominating(clockB).Should().BeTrue();
        clockA.IsDominatedBy(result.PrimaryVector).Should().BeTrue();
        clockB.IsDominatedBy(result.PrimaryVector).Should().BeTrue();
    }

    [Fact]
    public void Branch4b_SubsequentCollision_IncrementsCounter()
    {
        var clockA = VectorClock.Create(("NodeA", 1));
        var clockB = VectorClock.Create(("NodeB", 1));

        var existingFiles = new HashSet<string> { "doc (NodeB conflicted).txt" };

        var result = ConflictResolver.Resolve(
            localPeerId: "NodeA",
            relativePath: "doc.txt",
            localHash: "hashA",
            localClock: clockA,
            remotePeerId: "NodeB",
            remoteHash: "hashB",
            remoteClock: clockB,
            pathExists: p => existingFiles.Contains(p));

        result.Type.Should().Be(ConflictResolutionType.PreserveSideBySide);
        result.SiblingPath.Should().Be("doc (NodeB conflicted 2).txt");
    }

    [Theory]
    [InlineData(null, "doc.txt", "NodeB")]
    [InlineData("NodeA", null, "NodeB")]
    [InlineData("NodeA", "doc.txt", null)]
    [InlineData("   ", "doc.txt", "NodeB")]
    public void InvalidArguments_ThrowArgumentException(string? localId, string? path, string? remoteId)
    {
        Action act = () => ConflictResolver.Resolve(
            localPeerId: localId!,
            relativePath: path!,
            localHash: "h1",
            localClock: VectorClock.Empty,
            remotePeerId: remoteId!,
            remoteHash: "h2",
            remoteClock: VectorClock.Empty);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void NullRemoteClock_ThrowsArgumentNullException()
    {
        Action act = () => ConflictResolver.Resolve(
            localPeerId: "NodeA",
            relativePath: "doc.txt",
            localHash: "h1",
            localClock: VectorClock.Empty,
            remotePeerId: "NodeB",
            remoteHash: "h2",
            remoteClock: null!);

        act.Should().Throw<ArgumentNullException>();
    }
}
