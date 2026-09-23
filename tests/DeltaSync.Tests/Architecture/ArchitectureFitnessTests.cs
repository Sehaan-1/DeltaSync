using System.Reflection;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Storage;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Architecture;

public class ArchitectureFitnessTests
{
    private static readonly Assembly CoreAssembly = typeof(VectorClock).Assembly;
    private static readonly Assembly NetworkAssembly = typeof(PeerRegistry).Assembly;

    [Fact]
    public void Core_MustNotReference_NetworkOrCli_Test()
    {
        var referencedAssemblies = CoreAssembly.GetReferencedAssemblies()
            .Select(a => a.Name ?? string.Empty)
            .ToList();

        referencedAssemblies.Should().NotContain("DeltaSync.Network",
            "DeltaSync.Core must have zero dependencies on DeltaSync.Network per the Inward Dependency Rule.");
        referencedAssemblies.Should().NotContain("DeltaSync.Cli",
            "DeltaSync.Core must have zero dependencies on DeltaSync.Cli per the Inward Dependency Rule.");
    }

    [Fact]
    public void Network_MustNotReference_Cli_Test()
    {
        var referencedAssemblies = NetworkAssembly.GetReferencedAssemblies()
            .Select(a => a.Name ?? string.Empty)
            .ToList();

        referencedAssemblies.Should().NotContain("DeltaSync.Cli",
            "DeltaSync.Network must not depend on CLI or presentation concerns.");
    }

    [Fact]
    public void Causality_Types_MustNotReference_SqliteOrSockets_Test()
    {
        var causalityTypes = CoreAssembly.GetTypes()
            .Where(t => t.Namespace != null && t.Namespace.StartsWith("DeltaSync.Core.Causality"))
            .ToList();

        causalityTypes.Should().NotBeEmpty();

        foreach (var type in causalityTypes)
        {
            var fieldTypes = type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                .Select(f => f.FieldType.FullName ?? string.Empty);

            var methodParameterTypes = type.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                .SelectMany(m => m.GetParameters().Select(p => p.ParameterType.FullName ?? string.Empty));

            var allTypeNames = fieldTypes.Concat(methodParameterTypes).ToList();

            allTypeNames.Should().NotContain(name => name.Contains("Sqlite"),
                $"Causality type {type.Name} must be pure domain and never depend on SQLite.");
            allTypeNames.Should().NotContain(name => name.Contains("System.Net.Sockets"),
                $"Causality type {type.Name} must be pure domain and never depend on sockets.");
        }
    }

    [Fact]
    public void Chunking_Types_MustNotReference_SqliteOrSockets_Test()
    {
        var chunkingTypes = CoreAssembly.GetTypes()
            .Where(t => t.Namespace != null && t.Namespace.StartsWith("DeltaSync.Core.Chunking"))
            .ToList();

        chunkingTypes.Should().NotBeEmpty();

        foreach (var type in chunkingTypes)
        {
            var fieldTypes = type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                .Select(f => f.FieldType.FullName ?? string.Empty);

            var methodParameterTypes = type.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                .SelectMany(m => m.GetParameters().Select(p => p.ParameterType.FullName ?? string.Empty));

            var allTypeNames = fieldTypes.Concat(methodParameterTypes).ToList();

            allTypeNames.Should().NotContain(name => name.Contains("Sqlite"),
                $"Chunking type {type.Name} must be pure computational and never depend on SQLite.");
            allTypeNames.Should().NotContain(name => name.Contains("System.Net.Sockets"),
                $"Chunking type {type.Name} must be pure computational and never depend on sockets.");
        }
    }

    [Fact]
    public void Storage_Ports_MustBeDefinedInCore_Test()
    {
        (typeof(ISqliteStateStore).Assembly == CoreAssembly).Should().BeTrue(
            "ISqliteStateStore port must be owned by DeltaSync.Core.");
        (typeof(ILocalChunkProvider).Assembly == CoreAssembly).Should().BeTrue(
            "ILocalChunkProvider port must be owned by DeltaSync.Core.");
    }
}
