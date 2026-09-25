using DeltaSync.Cli;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public class StructuredCliCommandTests : IDisposable
{
    private readonly string _testDir;

    public StructuredCliCommandTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "deltasync_cli_test_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, recursive: true);
            }
        }
        catch
        {
            // Best effort cleanup
        }
    }

    [Fact]
    public void CliOptions_Parse_SyncCommandWithPositionalPath_Test()
    {
        string[] args = ["sync", @"C:\temp\vault", "--port", "4848", "--headless"];
        var options = CliOptions.Parse(args);

        options.Command.Should().Be(CliCommand.Sync);
        options.SyncPath.Should().Be(Path.GetFullPath(@"C:\temp\vault"));
        options.ListenPort.Should().Be(4848);
        options.IsHeadless.Should().BeTrue();
    }

    [Fact]
    public void CliOptions_Parse_PeerAddCommand_Test()
    {
        string[] args = ["peer", "add", "192.168.1.100:5000", "--path", @"C:\temp\vault"];
        var options = CliOptions.Parse(args);

        options.Command.Should().Be(CliCommand.Peer);
        options.PeerAction.Should().Be(PeerSubcommand.Add);
        options.PeerTarget.Should().Be("192.168.1.100:5000");
        options.SyncPath.Should().Be(Path.GetFullPath(@"C:\temp\vault"));
    }

    [Fact]
    public void CliOptions_Parse_PeerListCommand_Test()
    {
        string[] args = ["peer", "list", "--path", @"C:\temp\vault", "--json"];
        var options = CliOptions.Parse(args);

        options.Command.Should().Be(CliCommand.Peer);
        options.PeerAction.Should().Be(PeerSubcommand.List);
        options.SyncPath.Should().Be(Path.GetFullPath(@"C:\temp\vault"));
        options.JsonOutput.Should().BeTrue();
    }

    [Fact]
    public void CliOptions_Parse_PeerRemoveCommand_Test()
    {
        string[] args = ["peer", "remove", "127.0.0.1:4242"];
        var options = CliOptions.Parse(args);

        options.Command.Should().Be(CliCommand.Peer);
        options.PeerAction.Should().Be(PeerSubcommand.Remove);
        options.PeerTarget.Should().Be("127.0.0.1:4242");
    }

    [Fact]
    public void CliOptions_Parse_ConflictsCommand_Test()
    {
        string[] args = ["conflicts", @"C:\temp\vault", "--json"];
        var options = CliOptions.Parse(args);

        options.Command.Should().Be(CliCommand.Conflicts);
        options.SyncPath.Should().Be(Path.GetFullPath(@"C:\temp\vault"));
        options.JsonOutput.Should().BeTrue();
    }

    [Fact]
    public void CliOptions_Parse_StatusCommand_Test()
    {
        string[] args = ["status", @"C:\temp\vault"];
        var options = CliOptions.Parse(args);

        options.Command.Should().Be(CliCommand.Status);
        options.SyncPath.Should().Be(Path.GetFullPath(@"C:\temp\vault"));
    }

    [Fact]
    public async Task PeerConfigStore_AddListRemove_Workflow_Test()
    {
        // 1. Initially empty
        var initialPeers = await PeerConfigStore.LoadPeersAsync(_testDir);
        initialPeers.Should().BeEmpty();

        // 2. Add valid peer
        var (addSuccess, addMsg, peer) = await PeerConfigStore.AddPeerAsync(_testDir, "127.0.0.1:4242");
        addSuccess.Should().BeTrue();
        peer.Should().NotBeNull();
        peer!.Endpoint.Should().Be("127.0.0.1:4242");
        peer.Port.Should().Be(4242);

        // 3. List contains newly added peer
        var listed = await PeerConfigStore.LoadPeersAsync(_testDir);
        listed.Should().HaveCount(1);
        listed[0].Endpoint.Should().Be("127.0.0.1:4242");

        // 4. Duplicate add rejected
        var (dupSuccess, dupMsg, _) = await PeerConfigStore.AddPeerAsync(_testDir, "127.0.0.1:4242");
        dupSuccess.Should().BeFalse();
        dupMsg.Should().Contain("already configured");

        // 5. Remove peer
        var (remSuccess, remMsg) = await PeerConfigStore.RemovePeerAsync(_testDir, "127.0.0.1:4242");
        remSuccess.Should().BeTrue();

        // 6. List empty after remove
        var afterRemove = await PeerConfigStore.LoadPeersAsync(_testDir);
        afterRemove.Should().BeEmpty();
    }

    [Fact]
    public async Task PeerConfigStore_InvalidEndpoint_Rejected_Test()
    {
        var (success, msg, _) = await PeerConfigStore.AddPeerAsync(_testDir, "invalid-endpoint:notaport");
        success.Should().BeFalse();
        msg.Should().Contain("Invalid peer endpoint");
    }

    [Fact]
    public async Task ConflictScanner_ScansSideBySideConflicts_CorrectlyIdentifiesOriginal_Test()
    {
        // Create baseline file
        string originalPath = Path.Combine(_testDir, "specs.txt");
        await File.WriteAllTextAsync(originalPath, "Local authoritative baseline content.");

        // Create ADR-0001 side-by-side conflict copy
        string conflictPath1 = Path.Combine(_testDir, "specs (node-laptop conflicted).txt");
        await File.WriteAllTextAsync(conflictPath1, "Remote conflicting edit from laptop node.");

        // Create nested conflict copy without original existing
        string subDir = Path.Combine(_testDir, "sub");
        Directory.CreateDirectory(subDir);
        string conflictPath2 = Path.Combine(subDir, "orphan (peer-77 conflicted 2).md");
        await File.WriteAllTextAsync(conflictPath2, "Conflicted file where original is missing.");

        // Act
        var conflicts = await ConflictScanner.ScanConflictsAsync(_testDir);

        // Assert
        conflicts.Should().HaveCount(2);

        var first = conflicts.FirstOrDefault(c => c.RelativeOriginalPath == "specs.txt");
        first.Should().NotBeNull();
        first!.PeerId.Should().Be("node-laptop");
        first.OriginalExists.Should().BeTrue();
        first.RelativeConflictPath.Should().Be("specs (node-laptop conflicted).txt");
        first.ConflictSha256.Should().NotBeNullOrEmpty();
        first.OriginalSha256.Should().NotBeNullOrEmpty();
        first.Reason.Should().Contain("ADR-0001");

        var second = conflicts.FirstOrDefault(c => c.RelativeOriginalPath == "sub/orphan.md");
        second.Should().NotBeNull();
        second!.PeerId.Should().Be("peer-77");
        second.OriginalExists.Should().BeFalse();
        second.Reason.Should().Contain("missing or deleted");
    }

    [Fact]
    public async Task ConflictScanner_CleanDirectory_ReturnsEmpty_Test()
    {
        await File.WriteAllTextAsync(Path.Combine(_testDir, "clean.txt"), "No conflicts here.");
        var conflicts = await ConflictScanner.ScanConflictsAsync(_testDir);
        conflicts.Should().BeEmpty();
    }

    [Fact]
    public async Task Program_Main_PeerCommands_Workflow_Test()
    {
        // 1. Peer Add
        int addCode = await Program.Main(["peer", "add", "10.0.0.5:4242", "--path", _testDir]);
        addCode.Should().Be(0);

        // 2. Peer List
        int listCode = await Program.Main(["peer", "list", "--path", _testDir]);
        listCode.Should().Be(0);

        // 3. Peer List JSON
        int jsonListCode = await Program.Main(["peer", "list", "--path", _testDir, "--json"]);
        jsonListCode.Should().Be(0);

        // 4. Peer Remove
        int remCode = await Program.Main(["peer", "remove", "10.0.0.5:4242", "--path", _testDir]);
        remCode.Should().Be(0);
    }

    [Fact]
    public async Task Program_Main_ConflictsAndStatus_Test()
    {
        // Conflicts on clean directory
        int cleanConflictsCode = await Program.Main(["conflicts", _testDir]);
        cleanConflictsCode.Should().Be(0);

        // Status on directory
        int statusCode = await Program.Main(["status", _testDir]);
        statusCode.Should().Be(0);

        // Help
        int helpCode = await Program.Main(["help"]);
        helpCode.Should().Be(0);

        // Add conflict file and run conflicts again
        await File.WriteAllTextAsync(Path.Combine(_testDir, "doc.txt"), "Local");
        await File.WriteAllTextAsync(Path.Combine(_testDir, "doc (node-beta conflicted).txt"), "Remote");

        int dirtyConflictsCode = await Program.Main(["conflicts", _testDir]);
        dirtyConflictsCode.Should().Be(0);

        int jsonConflictsCode = await Program.Main(["conflicts", _testDir, "--json"]);
        jsonConflictsCode.Should().Be(0);
    }
}
