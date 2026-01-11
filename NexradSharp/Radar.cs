using System.Collections;
using System.Collections.Generic;
namespace NexradSharp;

public static class Radar
{
#pragma warning disable IDE1006
    public record struct FieldAttributes(FieldName name, float scale, float offset);
    public record struct SweepAttributes(int scanIndex, double startaz, double elangle, double rscale, double rstart);
    public record struct VolumeAttributes(DateTime datetime, double height, double latitude, double longitude);
#pragma warning restore IDE1006

    private interface IRadar<T1, T2>
    {
        public T1 Data { get; }
        public T2 Attributes { get; }
    }

    public class Field(Span2D<ushort> data, FieldAttributes attributes) :
        IRadar<Span2D<ushort>, FieldAttributes>
    {
        public Field(IEnumerable<ushort> data, FieldAttributes attributes, (int, int) shape) : this(new Span2D<ushort>(data, shape), attributes) { }
        public override string ToString() => $"Field([[{Data[0, 0]}, ...], ..., [..., {Data[^1, ^1]}]], ({data.NRays}, {data.NBins}))";
        public Span2D<ushort> Data => data;
        public FieldAttributes Attributes => attributes;
        public FieldName Name => attributes.name;
        public float Scale => attributes.scale;
        public float Offset => attributes.offset;

    }
    public class Sweep<T>(IReadOnlyDictionary<FieldName, T> data, SweepAttributes attributes) :
        IReadOnlyDictionary<FieldName, T>,
        IRadar<IReadOnlyDictionary<FieldName, T>, SweepAttributes>
        where T : Field
    {
        public IReadOnlyDictionary<FieldName, T> Data => data;
        public SweepAttributes Attributes => attributes;
        public int ScanIndex => attributes.scanIndex;
        public double StartAzimuth => attributes.startaz;
        public double ElevationAngle => attributes.elangle;
        public double RangeScale => attributes.rscale;
        public double RangeStart => attributes.rstart;

        // IReadOnlyDictionary implementation
        public T this[FieldName key]
        {
            get
            {
                if (TryGetValue(key, out var value)) return value;
                throw new KeyNotFoundException($"Key '{key}' not found in sweep data. Available keys: {string.Join(", ", Keys)}");
            }
        }
        public IEnumerable<FieldName> Keys => data.Keys;
        public IEnumerable<T> Values => data.Values;
        public int Count => data.Count;
        public bool ContainsKey(FieldName key) => data.ContainsKey(key);
        public bool TryGetValue(FieldName key, out T value) => data.TryGetValue(key, out value!);

        public T this[string key] => this[Enum.Parse<FieldName>(key)];
        public bool ContainsKey(string key) => data.ContainsKey(Enum.Parse<FieldName>(key));
        public bool TryGetValue(string key, out T value) => TryGetValue(Enum.Parse<FieldName>(key), out value!);
        public IEnumerator<KeyValuePair<FieldName, T>> GetEnumerator() => data.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public override string ToString()
        {
            var s = $"Sweep({ElevationAngle:F2}°, {StartAzimuth:F2}°, {RangeScale:F2}m, {RangeStart:F2}m)";
            int maxLength = Data.Keys.Max(k => k.ToString().Length);
            foreach (var (key, field) in Data)
            {
                var k = key.ToString();
                var padding = maxLength - k.Length;
                s += $"\n    {k}{new string(' ', padding)}: {field}";


            }
            return s;
        }

    }
    public class Volume<T>(IReadOnlyDictionary<int, T> data, VolumeAttributes attributes) :
        IReadOnlyDictionary<int, T>,
        IRadar<IReadOnlyDictionary<int, T>, VolumeAttributes>
        where T : Sweep<Field>
    {
        public Volume(IEnumerable<T> data, IEnumerable<int> indices, VolumeAttributes attributes)
            : this(indices.Zip(data).ToDictionary(kvp => kvp.First, kvp => kvp.Second), attributes)
        {
            if (data.Count() != indices.Count()) throw new ArgumentException("Data and indices must have the same length");
        }
        public Volume(IEnumerable<T> data, VolumeAttributes attributes) : this(data, Enumerable.Range(0, data.Count()), attributes) { }
        public IReadOnlyDictionary<int, T> Data => data;
        public VolumeAttributes Attributes => attributes;
        public DateTime DateTime => attributes.datetime;
        public double Height => attributes.height;
        public double Latitude => attributes.latitude;
        public double Longitude => attributes.longitude;

        // IReadOnlyDictionary implementation
        public T this[int scanIndex] => data[scanIndex];
        public IEnumerable<int> Keys => data.Keys;
        public IEnumerable<T> Values => data.Values;
        public int Count => data.Count;
        public bool ContainsKey(int key) => data.ContainsKey(key);
        public bool TryGetValue(int key, out T value) => data.TryGetValue(key, out value!);
        public IEnumerator<KeyValuePair<int, T>> GetEnumerator() => data.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public override string ToString()
        {
            var s = $"Volume({DateTime:yyyy-MM-dd HH:mm:ss}, {Height:F1}m, {Latitude:F2}°, {Longitude:F2}°)";
            foreach (var (k, v) in this)
                s += $"\n({k}) {v}";
            return s;
        }
    }

}

