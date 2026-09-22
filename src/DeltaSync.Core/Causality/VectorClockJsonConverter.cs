using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaSync.Core.Causality;

/// <summary>
/// JSON converter for <see cref="VectorClock"/> allowing clean serialization
/// as a standard key-value map (e.g. { "nodeA": 1, "nodeB": 2 }).
/// </summary>
public sealed class VectorClockJsonConverter : JsonConverter<VectorClock>
{
    public override VectorClock Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return VectorClock.Empty;
        }

        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException($"Expected StartObject token, got {reader.TokenType}.");
        }

        var dict = new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase);

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                return VectorClock.Create(dict);
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException($"Expected PropertyName token, got {reader.TokenType}.");
            }

            string propertyName = reader.GetString() ?? throw new JsonException("Property name cannot be null.");

            reader.Read();
            if (reader.TokenType != JsonTokenType.Number)
            {
                throw new JsonException($"Expected Number token for counter of '{propertyName}', got {reader.TokenType}.");
            }

            ulong counter = reader.GetUInt64();
            dict[propertyName] = counter;
        }

        throw new JsonException("Unexpected end of JSON stream while deserializing VectorClock.");
    }

    public override void Write(Utf8JsonWriter writer, VectorClock value, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(value);

        writer.WriteStartObject();
        // Deterministic serialization: write entries in sorted key order
        foreach (var key in value.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase))
        {
            writer.WriteNumber(key, value[key]);
        }
        writer.WriteEndObject();
    }
}
