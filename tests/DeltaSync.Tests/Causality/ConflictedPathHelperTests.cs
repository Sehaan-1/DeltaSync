using DeltaSync.Core.Causality;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Causality;

public class ConflictedPathHelperTests
{
    [Fact]
    public void StandardFile_GeneratesConflictedPathWithPeer()
    {
        string path = ConflictedPathHelper.GenerateConflictedPath("doc.txt", "NodeB");
        path.Should().Be("doc (NodeB conflicted).txt");
    }

    [Fact]
    public void ExistingConflictedPath_IncrementsCollisionCounter()
    {
        var existing = new HashSet<string> { "doc (NodeB conflicted).txt" };
        string path = ConflictedPathHelper.GenerateConflictedPath("doc.txt", "NodeB", p => existing.Contains(p));
        path.Should().Be("doc (NodeB conflicted 2).txt");
    }

    [Fact]
    public void MultipleExisting_IncrementsToFirstAvailable()
    {
        var existing = new HashSet<string>
        {
            "doc (NodeB conflicted).txt",
            "doc (NodeB conflicted 2).txt",
            "doc (NodeB conflicted 3).txt"
        };
        string path = ConflictedPathHelper.GenerateConflictedPath("doc.txt", "NodeB", p => existing.Contains(p));
        path.Should().Be("doc (NodeB conflicted 4).txt");
    }

    [Fact]
    public void FileWithoutExtension_GeneratesConflictedPathCorrectly()
    {
        string path = ConflictedPathHelper.GenerateConflictedPath("Makefile", "MachineA");
        path.Should().Be("Makefile (MachineA conflicted)");
    }

    [Fact]
    public void DotFile_GeneratesConflictedPathCorrectly()
    {
        string path = ConflictedPathHelper.GenerateConflictedPath(".gitignore", "NodeB");
        path.Should().Be(".gitignore (NodeB conflicted)");
    }

    [Fact]
    public void NestedSubdirectory_PreservesDirectoryStructure()
    {
        string path = ConflictedPathHelper.GenerateConflictedPath("src/docs/notes.md", "LaptopX");
        path.Should().Be("src/docs/notes (LaptopX conflicted).md");
    }

    [Fact]
    public void BackslashPath_NormalizedToForwardSlash()
    {
        string path = ConflictedPathHelper.GenerateConflictedPath(@"src\docs\notes.md", "LaptopX");
        path.Should().Be("src/docs/notes (LaptopX conflicted).md");
    }

    [Fact]
    public void PeerIdWithInvalidChars_SanitizesChars()
    {
        string path = ConflictedPathHelper.GenerateConflictedPath("test.txt", "Node:B?*");
        path.Should().Be("test (Node_B__ conflicted).txt");
    }

    [Fact]
    public void Exceeding100Attempts_ThrowsInvalidOperationException()
    {
        Action act = () => ConflictedPathHelper.GenerateConflictedPath("test.txt", "NodeB", _ => true);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*100*");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void InvalidInputs_ThrowArgumentException(string? invalid)
    {
        Action act1 = () => ConflictedPathHelper.GenerateConflictedPath(invalid!, "NodeB");
        act1.Should().Throw<ArgumentException>();

        Action act2 = () => ConflictedPathHelper.GenerateConflictedPath("test.txt", invalid!);
        act2.Should().Throw<ArgumentException>();
    }
}
