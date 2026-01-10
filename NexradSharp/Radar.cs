using System.Collections;
using System.Collections.Generic;
namespace NexradSharp;
using ScaleOffset = (float scale, float offset);
using SweepAttributes = (int scanIndex, double startaz, double elangle, double rscale, double rstart);
using VolumeAttributes = (DateTime datetime, double height, double latitude, double longitude);
public static class Radar
{
    private interface IRadar<T1, T2>
    {
        public T1 Data { get; }
        public T2 Attributes { get; }
    }

    public class Field(Span2D<ushort> data, ScaleOffset attributes) :
        IRadar<Span2D<ushort>, ScaleOffset>
    {
        public override string ToString() => $"Field([[{Data[0, 0]}, ...], ..., [..., {Data[^1, ^1]}]], ({data.NRays}, {data.NBins}))";
        public Span2D<ushort> Data => data;
        public ScaleOffset Attributes => attributes;
        public float Scale => attributes.scale;
        public float Offset => attributes.offset;
        public float[,] Dequantize()
        {
            var (nrays, nbins) = data.Shape;
            var (scale, offset) = attributes;
            var result = new float[nrays, nbins];
            for (int i = 0; i < nrays; i++)
                for (int j = 0; j < nbins; j++)
                    result[i, j] = (data[i, j] - offset) * scale;

            return result;
        }
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

