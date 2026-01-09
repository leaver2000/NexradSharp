using System.Collections;
using System.Linq;
using System.Numerics;
namespace NexradSharp;

using ScaleOffset = (float scale, float offset);


/// <summary>
/// Common interface for accessing NEXRAD data with key-based access.
/// </summary>
public interface IDataAccessor<K, T>
{
    /// <summary>
    /// Gets all available keys in this data accessor.
    /// </summary>
    IEnumerable<K> Keys { get; }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key">The key to look up</param>
    /// <returns>The value associated with the key</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the key is not found</exception>
    T this[K key] { get; }

    /// <summary>
    /// Attempts to get the value associated with the specified key.
    /// </summary>
    /// <param name="key">The key to look up</param>
    /// <param name="value">When this method returns, contains the value associated with the key, if found</param>
    /// <returns>True if the key was found; otherwise, false</returns>
    bool TryGetValue(K key, out T value);

    /// <summary>
    /// Determines whether the accessor contains the specified key.
    /// </summary>
    /// <param name="key">The key to locate</param>
    /// <returns>True if the key exists; otherwise, false</returns>
    bool ContainsKey(K key);
}



/// <summary>
/// Provides key-based access to sweep data variables (e.g., DBZH, VRADH, WRADH).
/// Inherits from Radar.Sweep and provides convenient access to data and attributes.
/// </summary>
public class NexradLevel2Sweep(
    Dictionary<CommonName, Radar.Field> data,
    int scanIndex,
    double startAzimuth,
    double elevationAngle,
    double rangeScale,
    double rangeStart
) : Radar.Sweep(data, (scanIndex, startAzimuth, elangle: elevationAngle, rscale: rangeScale, rstart: rangeStart)),
    IDataAccessor<CommonName, Radar.Field>, IEnumerable<KeyValuePair<CommonName, Radar.Field>>
{
    public override string ToString() => $"NexradLevel2Sweep[[{string.Join(", ", Keys)}]]";

    public IEnumerable<CommonName> Keys => Data.Keys;

    public Radar.Field this[CommonName key]
    {
        get
        {
            if (TryGetValue(key, out var value))
                return value;
            throw new KeyNotFoundException($"Key '{key}' not found in sweep data. Available keys: {string.Join(", ", Keys)}");
        }
    }

    /// <summary>
    /// Convenience indexer that returns just the data (Span2D) for string keys.
    /// </summary>
    public Radar.Field this[string key] => this[Enum.Parse<CommonName>(key)];

    public bool TryGetValue(CommonName key, out Radar.Field value) => Data.TryGetValue(key, out value!);

    /// <summary>
    /// Convenience method that returns just the data (Span2D) for string keys.
    /// </summary>
    public bool TryGetValue(string key, out Span2D<ushort> value)
    {
        if (TryGetValue(Enum.Parse<CommonName>(key), out var field))
        {
            value = field.Data;
            return true;
        }
        value = default!;
        return false;
    }

    public bool ContainsKey(CommonName key) => Data.ContainsKey(key);

    /// <summary>
    /// Convenience method for string keys.
    /// </summary>
    public bool ContainsKey(string key) => ContainsKey(Enum.Parse<CommonName>(key));

    /// <summary>
    /// Gets the number of variables in this sweep.
    /// </summary>
    public int Count => Data.Count;

    /// <summary>
    /// Gets the scale and offset for a given variable key.
    /// </summary>
    /// <param name="key">The variable key (e.g., CommonName.DBZH, CommonName.VRADH)</param>
    /// <returns>A tuple containing (scale, offset) for dequantization</returns>
    public ScaleOffset GetScaleOffset(CommonName key)
    {
        if (Data.TryGetValue(key, out var field))
            return field.Attributes;
        throw new KeyNotFoundException($"Scale/offset not found for key '{key}'");
    }

    /// <summary>
    /// Attempts to get the scale and offset for a given variable key.
    /// </summary>
    /// <param name="key">The variable key</param>
    /// <param name="scaleOffset">When this method returns, contains the scale and offset if found</param>
    /// <returns>True if the scale/offset was found; otherwise, false</returns>
    public bool TryGetScaleOffset(CommonName key, out (float scale, float offset) scaleOffset)
    {
        if (Data.TryGetValue(key, out var field))
        {
            scaleOffset = field.Attributes;
            return true;
        }
        scaleOffset = default;
        return false;
    }

    public IEnumerator<KeyValuePair<CommonName, Radar.Field>> GetEnumerator() => Data.GetEnumerator();

    IEnumerator<KeyValuePair<CommonName, Radar.Field>> IEnumerable<KeyValuePair<CommonName, Radar.Field>>.GetEnumerator() => Data.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => Data.GetEnumerator();


}

/// <summary>
/// Provides key-based access to multiple sweeps.
/// Inherits from Radar.Volume and provides convenient access to data and attributes.
/// </summary>
public class NexradLevel2Volume : Radar.Volume, IDataAccessor<int, NexradLevel2Sweep>, IEnumerable<NexradLevel2Sweep>
{
    private readonly Dictionary<int, NexradLevel2Sweep> _sweeps;

    public NexradLevel2Volume(
        Dictionary<int, Dictionary<CommonName, Radar.Field>> sweepData,
        DateTime timestamp,
        double height,
        double latitude,
        double longitude,
        IReadOnlyList<double> elevationAngles,
        Dictionary<int, (short rangeStart, short rangeScale)> sweepRangeInfo
    ) : base(
        CreateSweeps(sweepData, elevationAngles, sweepRangeInfo),
        (datetime: timestamp, height, latitude, longitude)
    )
    {
        // Store NexradLevel2Sweep instances for convenient access
        _sweeps = Data.ToDictionary(
            kvp => kvp.Key,
            kvp => (NexradLevel2Sweep)kvp.Value
        );
    }

    private static Dictionary<int, Radar.Sweep> CreateSweeps(
        Dictionary<int, Dictionary<CommonName, Radar.Field>> sweepData,
        IReadOnlyList<double> elevationAngles,
        Dictionary<int, (short rangeStart, short rangeScale)> sweepRangeInfo
    )
    {
        return sweepData.ToDictionary(
            kvp => kvp.Key,
            kvp =>
            {
                var scanIndex = kvp.Key;
                var (rangeStart, rangeScale) = sweepRangeInfo[kvp.Key];
                var elevationAngle = elevationAngles[scanIndex];
                return (Radar.Sweep)new NexradLevel2Sweep(
                    kvp.Value,
                    scanIndex,
                    startAzimuth: 0.0,
                    elevationAngle,
                    rangeScale,
                    rangeStart
                );
            }
        );
    }

    // Convenience properties for backward compatibility
    public DateTime Timestamp => Attributes.datetime;
    public IReadOnlyList<double> ElevationAngles => _sweeps.Values.Select(s => s.ElevationAngle).ToList().AsReadOnly();

    public IEnumerable<int> Keys => Data.Keys;

    public NexradLevel2Sweep this[int key]
    {
        get
        {
            if (_sweeps.TryGetValue(key, out var value))
                return value;
            throw new KeyNotFoundException($"Sweep {key} not found. Available sweeps: {string.Join(", ", Keys)}");
        }
    }

    public bool TryGetValue(int key, out NexradLevel2Sweep value)
    {
        return _sweeps.TryGetValue(key, out value!);
    }

    public bool ContainsKey(int key) => Data.ContainsKey(key);

    /// <summary>
    /// Gets the number of sweeps.
    /// </summary>
    public int Count => _sweeps.Count;

    /// <summary>
    /// Gets a sweep by index (0-based).
    /// </summary>
    public NexradLevel2Sweep GetSweep(int index)
    {
        if (_sweeps.TryGetValue(index, out var sweep))
            return sweep;
        throw new IndexOutOfRangeException($"Sweep index {index} is out of range. Available sweeps: {string.Join(", ", Keys)}");
    }
    public IEnumerator<NexradLevel2Sweep> GetEnumerator() => _sweeps.Values.GetEnumerator();

    IEnumerator<NexradLevel2Sweep> IEnumerable<NexradLevel2Sweep>.GetEnumerator() => _sweeps.Values.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _sweeps.Values.GetEnumerator();
}
