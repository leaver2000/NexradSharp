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

    public class Field(Span2D<ushort> data, ScaleOffset attributes) : IRadar<Span2D<ushort>, ScaleOffset>
    {
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
    public class Sweep(Dictionary<CommonName, Field> data, SweepAttributes attributes) : IRadar<Dictionary<CommonName, Field>, SweepAttributes>
    {
        public Dictionary<CommonName, Field> Data => data;
        public SweepAttributes Attributes => attributes;
        public int ScanIndex => attributes.scanIndex;
        public double StartAzimuth => attributes.startaz;
        public double ElevationAngle => attributes.elangle;
        public double RangeScale => attributes.rscale;
        public double RangeStart => attributes.rstart;
    }
    public class Volume(Dictionary<int, Sweep> data, VolumeAttributes attributes) : IRadar<Dictionary<int, Sweep>, VolumeAttributes>
    {
        public Dictionary<int, Sweep> Data => data;
        public VolumeAttributes Attributes => attributes;
        public DateTime DateTime => attributes.datetime;
        public double Height => attributes.height;
        public double Latitude => attributes.latitude;
        public double Longitude => attributes.longitude;
    }

}

