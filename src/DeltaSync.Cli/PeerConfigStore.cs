using System.Text.Json;
using System.Text.Json.Serialization;
using DeltaSync.Network;

namespace DeltaSync.Cli;

public sealed record ConfiguredPeer(
    [property: JsonPropertyName("endpoint")] string Endpoint,
    [property: JsonPropertyName("host")] string Host,
    [property: JsonPropertyName("port")] int Port,
    [property: JsonPropertyName("addedAt")] DateTimeOffset AddedAt);

public sealed class PeerConfigFile
{
    [JsonPropertyName("peers")]
    public List<ConfiguredPeer> Peers { get; set; } = new();
}

/// <summary>
/// Crash-safe persistent storage of static peer endpoints inside .deltasync/peers.json (ADR-0002).
/// </summary>
public static class PeerConfigStore
{
    public static string GetConfigPath(string syncPath) =>
        Path.Combine(syncPath, ".deltasync", "peers.json");

    public static async Task<IReadOnlyList<ConfiguredPeer>> LoadPeersAsync(string syncPath, CancellationToken ct = default)
    {
        string filePath = GetConfigPath(syncPath);
        if (!File.Exists(filePath))
        {
            return Array.Empty<ConfiguredPeer>();
        }

        try
        {
            await using var stream = File.OpenRead(filePath);
            var doc = await JsonSerializer.DeserializeAsync<PeerConfigFile>(stream, cancellationToken: ct);
            return doc?.Peers ?? (IReadOnlyList<ConfiguredPeer>)Array.Empty<ConfiguredPeer>();
        }
        catch
        {
            return Array.Empty<ConfiguredPeer>();
        }
    }

    public static async Task SavePeersAsync(string syncPath, IReadOnlyList<ConfiguredPeer> peers, CancellationToken ct = default)
    {
        string filePath = GetConfigPath(syncPath);
        string dir = Path.GetDirectoryName(filePath)!;
        Directory.CreateDirectory(dir);

        var doc = new PeerConfigFile { Peers = peers.ToList() };
        var options = new JsonSerializerOptions { WriteIndented = true };

        string tmpFile = filePath + ".tmp";
        await using (var stream = File.Create(tmpFile))
        {
            await JsonSerializer.SerializeAsync(stream, doc, options, ct);
        }
        File.Move(tmpFile, filePath, overwrite: true);
    }

    public static async Task<(bool Success, string Message, ConfiguredPeer? Peer)> AddPeerAsync(string syncPath, string endpointString, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(endpointString))
        {
            return (false, "Peer endpoint address cannot be empty.", null);
        }

        if (!StaticPeerProvider.TryParseEndpoint(endpointString.Trim(), out var endPoint, out var error))
        {
            return (false, $"Invalid peer endpoint '{endpointString}': {error}", null);
        }

        var peers = (await LoadPeersAsync(syncPath, ct)).ToList();

        if (peers.Any(p => string.Equals(p.Endpoint, endpointString.Trim(), StringComparison.OrdinalIgnoreCase) ||
                           (string.Equals(p.Host, endPoint.Address.ToString(), StringComparison.OrdinalIgnoreCase) && p.Port == endPoint.Port)))
        {
            return (false, $"Peer '{endpointString.Trim()}' is already configured in {GetConfigPath(syncPath)}.", null);
        }

        var newPeer = new ConfiguredPeer(
            Endpoint: endpointString.Trim(),
            Host: endPoint.Address.ToString(),
            Port: endPoint.Port,
            AddedAt: DateTimeOffset.UtcNow);

        peers.Add(newPeer);
        await SavePeersAsync(syncPath, peers, ct);
        return (true, $"Successfully added peer '{endpointString.Trim()}'.", newPeer);
    }

    public static async Task<(bool Success, string Message)> RemovePeerAsync(string syncPath, string endpointString, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(endpointString))
        {
            return (false, "Peer endpoint address cannot be empty.");
        }

        var peers = (await LoadPeersAsync(syncPath, ct)).ToList();
        int removed = peers.RemoveAll(p =>
            string.Equals(p.Endpoint, endpointString.Trim(), StringComparison.OrdinalIgnoreCase) ||
            string.Equals($"{p.Host}:{p.Port}", endpointString.Trim(), StringComparison.OrdinalIgnoreCase));

        if (removed == 0)
        {
            return (false, $"Peer '{endpointString.Trim()}' was not found in configuration.");
        }

        await SavePeersAsync(syncPath, peers, ct);
        return (true, $"Successfully removed peer '{endpointString.Trim()}'.");
    }
}
