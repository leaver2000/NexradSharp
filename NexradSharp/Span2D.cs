using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NexradSharp;
using Shape = (int nrays, int nbins);
/// <summary>
/// A 2D span of numbers.
/// </summary>
/// <typeparam name="T">The type of the numbers in the span.</typeparam>
public class Span2D<T>(IEnumerable<T> data, Shape shape) where T : unmanaged, INumber<T>
{
    #region properties
    /// <summary>
    /// The data in the span.
    /// </summary>
    private readonly T[] _data = data.ToArray() ?? throw new ArgumentNullException(nameof(data));
    public readonly int NRays = shape.nrays;
    public readonly int NBins = shape.nbins;
    /// <summary>
    /// The shape of the span.
    /// </summary>
    public Shape Shape => (NRays, NBins);
    /// <summary>
    /// Stride of the span.
    /// </summary>
    public int Length => NRays * NBins;
    #endregion
    #region constructors
    public Span2D(IEnumerable<T> data, int nrays, int nbins) : this(data, (nrays, nbins)) { }
    public static Span2D<T> FromBytes(Span<byte> bytes, Shape shape, Func<ReadOnlySpan<byte>, T> converter)
    {
        var data = new T[shape.nrays * shape.nbins];
        int size = Unsafe.SizeOf<T>();
        int start = 0;
        int stop = size;
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = converter(bytes[start..stop]);
            start = stop;
            stop += size;
        }
        return new(data, shape);
    }
    public static Span2D<T> FromBytes(byte[] bytes, (int nrays, int nbins) shape, Func<ReadOnlySpan<byte>, T> converter)
        => FromBytes(bytes.AsSpan(), shape, converter);


    #endregion
    public T this[int ray, int bin] => _data[ray * NBins + bin];
    public T this[int ray, Index bin] => this[ray, bin.GetOffset(NBins)];
    public T this[Index ray, int bin] => this[ray.GetOffset(NRays), bin];
    public T this[Index ray, Index bin] => this[ray.GetOffset(NRays), bin.GetOffset(NBins)];


    public override string ToString() => $"Span2D([[...], ...], ({Shape.nrays}, {Shape.nbins}))";
}


