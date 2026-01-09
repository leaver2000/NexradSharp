using System.Collections;

namespace NexradSharp;


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
/// </summary>
public class NexradLevel2Sweep(
    Dictionary<string, float[,]> data, double elevationAngle, double altitude, short rangeStart, short rangeScale, DateTime timestamp
) : IDataAccessor<string, float[,]>, IEnumerable<KeyValuePair<string, float[,]>>
{

    public readonly double ElevationAngle = elevationAngle;
    public readonly double Altitude = altitude;
    public readonly short RangeStart = rangeStart;
    public readonly short RangeScale = rangeScale;
    public readonly DateTime Timestamp = timestamp;
    private readonly Dictionary<string, float[,]> _data = data ?? throw new ArgumentNullException(nameof(data));

    public IEnumerable<string> Keys => _data.Keys;

    public float[,] this[string key]
    {
        get
        {
            if (TryGetValue(key, out var value))
                return value;
            throw new KeyNotFoundException($"Key '{key}' not found in sweep data. Available keys: {string.Join(", ", Keys)}");
        }
    }

    public bool TryGetValue(string key, out float[,] value)
    {
        return _data.TryGetValue(key, out value!);
    }

    public bool ContainsKey(string key) => _data.ContainsKey(key);

    /// <summary>
    /// Gets the number of variables in this sweep.
    /// </summary>
    public int Count => _data.Count;

    public IEnumerator<KeyValuePair<string, float[,]>> GetEnumerator() => _data.GetEnumerator();

    IEnumerator<KeyValuePair<string, float[,]>> IEnumerable<KeyValuePair<string, float[,]>>.GetEnumerator() => _data.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _data.GetEnumerator();


}

/// <summary>
/// Provides key-based access to multiple sweeps.
/// </summary>
public class NexradLevel2Volume(
    Dictionary<int, Dictionary<string, float[,]>> data,
    IReadOnlyList<double> elevationAngles,
    Dictionary<int, (double altitude, short rangeStart, short rangeScale, DateTime timestamp)> sweepMetadata
) : IDataAccessor<int, NexradLevel2Sweep>, IEnumerable<NexradLevel2Sweep>
{
    public readonly IReadOnlyList<double> ElevationAngles = elevationAngles;
    private readonly Dictionary<int, NexradLevel2Sweep> _sweeps = data.ToDictionary(
            kvp => kvp.Key,
            kvp =>
            {
                var (altitude, rangeStart, rangeScale, timestamp) = sweepMetadata[kvp.Key];
                return new NexradLevel2Sweep(kvp.Value, elevationAngles[kvp.Key], altitude, rangeStart, rangeScale, timestamp);
            }
        );

    public IEnumerable<int> Keys => _sweeps.Keys;

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

    public bool ContainsKey(int key) => _sweeps.ContainsKey(key);

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

