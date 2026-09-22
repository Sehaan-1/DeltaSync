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

    #region Spec §10 Check 3: Concurrent Edit Convergence Simulation

    [Fact]
    public void SpecSection10_Check3_ConcurrentEditConvergenceSimulation()
    {
        string dirA = Path.Combine(Path.GetTempPath(), "DeltaSync_Check3_NodeA_" + Guid.NewGuid());
        string dirB = Path.Combine(Path.GetTempPath(), "DeltaSync_Check3_NodeB_" + Guid.NewGuid());
        Directory.CreateDirectory(dirA);
        Directory.CreateDirectory(dirB);

        try
        {
            // 1. Disconnected phase: Node A and Node B edit doc.txt independently
            byte[] contentA = System.Text.Encoding.UTF8.GetBytes("Node A independent edit");
            byte[] contentB = System.Text.Encoding.UTF8.GetBytes("Node B independent edit");

            string pathA = Path.Combine(dirA, "doc.txt");
            string pathB = Path.Combine(dirB, "doc.txt");
            File.WriteAllBytes(pathA, contentA);
            File.WriteAllBytes(pathB, contentB);

            string hashA = "sha256-node-a-content";
            string hashB = "sha256-node-b-content";

            var clockA = VectorClock.Create(("NodeA", 1));
            var clockB = VectorClock.Create(("NodeB", 1));

            // State dictionaries representing peer sync index
            var indexA = new Dictionary<string, (string Hash, VectorClock Clock)>
            {
                ["doc.txt"] = (hashA, clockA)
            };
            var indexB = new Dictionary<string, (string Hash, VectorClock Clock)>
            {
                ["doc.txt"] = (hashB, clockB)
            };

            // 2. Reconnect: Node B transmits state to Node A
            var resOnA = ConflictResolver.ResolveForDirectory(
                baseDirectory: dirA,
                localPeerId: "NodeA",
                relativePath: "doc.txt",
                localHash: indexA["doc.txt"].Hash,
                localClock: indexA["doc.txt"].Clock,
                remotePeerId: "NodeB",
                remoteHash: hashB,
                remoteClock: clockB);

            // Assert Conflict resolution on Node A is PreserveSideBySide
            resOnA.Type.Should().Be(ConflictResolutionType.PreserveSideBySide);
            resOnA.CausalRelation.Should().Be(CausalRelation.Concurrent);
            resOnA.PrimaryPath.Should().Be("doc.txt");
            resOnA.SiblingPath.Should().Be("doc (NodeB conflicted).txt");

            // Apply resolution on Node A's directory
            ConflictResolver.ApplyToDirectory(resOnA, dirA, contentB);

            // Update Node A's local index
            indexA["doc.txt"] = (hashA, resOnA.PrimaryVector);
            indexA[resOnA.SiblingPath!] = (hashB, resOnA.SiblingVector!);

            // Assertions on Node A:
            // a. doc.txt exists with Node A's content
            File.Exists(pathA).Should().BeTrue();
            File.ReadAllBytes(pathA).Should().Equal(contentA);

            // b. doc (NodeB conflicted).txt exists with Node B's content
            string siblingPathA = Path.Combine(dirA, "doc (NodeB conflicted).txt");
            File.Exists(siblingPathA).Should().BeTrue();
            File.ReadAllBytes(siblingPathA).Should().Equal(contentB);

            // c. Node A's vector clock strictly dominates Node B's original vector clock (VB < V'A)
            var unifiedVectorA = resOnA.PrimaryVector;
            unifiedVectorA.IsDominating(clockB).Should().BeTrue();
            clockB.IsDominatedBy(unifiedVectorA).Should().BeTrue();
            (clockB < unifiedVectorA).Should().BeTrue();

            // 3. Bidirectional convergence: Node A syncs its full state back to Node B
            foreach (var kvp in indexA)
            {
                string relPath = kvp.Key;
                var (remoteHash, remoteClock) = kvp.Value;
                byte[] remoteContent = File.ReadAllBytes(Path.Combine(dirA, relPath));

                indexB.TryGetValue(relPath, out var localBEntry);

                var resOnB = ConflictResolver.ResolveForDirectory(
                    baseDirectory: dirB,
                    localPeerId: "NodeB",
                    relativePath: relPath,
                    localHash: localBEntry.Hash,
                    localClock: localBEntry.Clock,
                    remotePeerId: "NodeA",
                    remoteHash: remoteHash,
                    remoteClock: remoteClock);

                // Crucial assertion: Node B must NOT raise a second conflict!
                resOnB.Type.Should().Be(ConflictResolutionType.ApplyRemote);
                resOnB.CausalRelation.Should().Be(CausalRelation.Before);

                ConflictResolver.ApplyToDirectory(resOnB, dirB, remoteContent);
                indexB[relPath] = (remoteHash, resOnB.PrimaryVector);
            }

            // Assert Node B converges to the identical two files
            string docOnB = Path.Combine(dirB, "doc.txt");
            string siblingOnB = Path.Combine(dirB, "doc (NodeB conflicted).txt");

            File.Exists(docOnB).Should().BeTrue();
            File.ReadAllBytes(docOnB).Should().Equal(contentA);

            File.Exists(siblingOnB).Should().BeTrue();
            File.ReadAllBytes(siblingOnB).Should().Equal(contentB);

            // Assert vectors on Node B match Node A's unified vectors exactly
            (indexB["doc.txt"].Clock == unifiedVectorA).Should().BeTrue();
            (indexB["doc (NodeB conflicted).txt"].Clock == clockB).Should().BeTrue();
        }
        finally
        {
            if (Directory.Exists(dirA)) Directory.Delete(dirA, true);
            if (Directory.Exists(dirB)) Directory.Delete(dirB, true);
        }
    }

    [Fact]
    public void ThreeWayConcurrentPartition_PreservesAllRevisions_AndConverges()
    {
        string dirA = Path.Combine(Path.GetTempPath(), "DeltaSync_3Way_A_" + Guid.NewGuid());
        string dirB = Path.Combine(Path.GetTempPath(), "DeltaSync_3Way_B_" + Guid.NewGuid());
        string dirC = Path.Combine(Path.GetTempPath(), "DeltaSync_3Way_C_" + Guid.NewGuid());
        Directory.CreateDirectory(dirA);
        Directory.CreateDirectory(dirB);
        Directory.CreateDirectory(dirC);

        try
        {
            byte[] cA = System.Text.Encoding.UTF8.GetBytes("Data A");
            byte[] cB = System.Text.Encoding.UTF8.GetBytes("Data B");
            byte[] cC = System.Text.Encoding.UTF8.GetBytes("Data C");

            File.WriteAllBytes(Path.Combine(dirA, "notes.txt"), cA);
            File.WriteAllBytes(Path.Combine(dirB, "notes.txt"), cB);
            File.WriteAllBytes(Path.Combine(dirC, "notes.txt"), cC);

            var vA = VectorClock.Create(("NodeA", 1));
            var vB = VectorClock.Create(("NodeB", 1));
            var vC = VectorClock.Create(("NodeC", 1));

            var indexA = new Dictionary<string, (string Hash, VectorClock Clock)>
            {
                ["notes.txt"] = ("hA", vA)
            };

            // 1. Sync Node B to Node A
            var resBtoA = ConflictResolver.ResolveForDirectory(dirA, "NodeA", "notes.txt", "hA", indexA["notes.txt"].Clock, "NodeB", "hB", vB);
            resBtoA.Type.Should().Be(ConflictResolutionType.PreserveSideBySide);
            ConflictResolver.ApplyToDirectory(resBtoA, dirA, cB);
            indexA["notes.txt"] = ("hA", resBtoA.PrimaryVector);
            indexA[resBtoA.SiblingPath!] = ("hB", resBtoA.SiblingVector!);

            // 2. Sync Node C to Node A
            var resCtoA = ConflictResolver.ResolveForDirectory(dirA, "NodeA", "notes.txt", "hA", indexA["notes.txt"].Clock, "NodeC", "hC", vC);
            resCtoA.Type.Should().Be(ConflictResolutionType.PreserveSideBySide);
            ConflictResolver.ApplyToDirectory(resCtoA, dirA, cC);
            indexA["notes.txt"] = ("hA", resCtoA.PrimaryVector);
            indexA[resCtoA.SiblingPath!] = ("hC", resCtoA.SiblingVector!);

            // Assert Node A preserves all 3 copies on disk!
            File.Exists(Path.Combine(dirA, "notes.txt")).Should().BeTrue();
            File.Exists(Path.Combine(dirA, "notes (NodeB conflicted).txt")).Should().BeTrue();
            File.Exists(Path.Combine(dirA, "notes (NodeC conflicted).txt")).Should().BeTrue();

            File.ReadAllBytes(Path.Combine(dirA, "notes.txt")).Should().Equal(cA);
            File.ReadAllBytes(Path.Combine(dirA, "notes (NodeB conflicted).txt")).Should().Equal(cB);
            File.ReadAllBytes(Path.Combine(dirA, "notes (NodeC conflicted).txt")).Should().Equal(cC);

            // Node A's main vector clock absorbed all branches: {NodeA: 3, NodeB: 1, NodeC: 1}
            var finalVectorA = indexA["notes.txt"].Clock;
            var expectedFinal = VectorClock.Create(("NodeA", 3), ("NodeB", 1), ("NodeC", 1));
            (finalVectorA == expectedFinal).Should().BeTrue();

            // Strictly dominates vA, vB, vC
            finalVectorA.IsDominating(vA).Should().BeTrue();
            finalVectorA.IsDominating(vB).Should().BeTrue();
            finalVectorA.IsDominating(vC).Should().BeTrue();
        }
        finally
        {
            if (Directory.Exists(dirA)) Directory.Delete(dirA, true);
            if (Directory.Exists(dirB)) Directory.Delete(dirB, true);
            if (Directory.Exists(dirC)) Directory.Delete(dirC, true);
        }
    }

    #endregion
}
