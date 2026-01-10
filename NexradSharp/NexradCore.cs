using System.Collections;

namespace NexradSharp;


public class NexradLevel2Sweep(
    IReadOnlyDictionary<FieldName, Radar.Field> data,
    int scanIndex,
    double startAzimuth,
    double elevationAngle,
    double rangeScale,
    double rangeStart
) : Radar.Sweep<Radar.Field>(data, (scanIndex, startAzimuth, elangle: elevationAngle, rscale: rangeScale, rstart: rangeStart))
{ }

/// <summary>
/// Provides key-based access to multiple sweeps.
/// Inherits from Radar.Volume and provides convenient access to data and attributes.
/// </summary>
public class NexradLevel2Volume : Radar.Volume<NexradLevel2Sweep>
{


    public NexradLevel2Volume(
        Dictionary<int, Dictionary<FieldName, Radar.Field>> sweepData,
        DateTime datetime,
        double height,
        double latitude,
        double longitude,
        IReadOnlyList<double> elevationAngles,
        IReadOnlyList<double> startAzimuths,
        Dictionary<int, (short rangeStart, short rangeScale)> sweepRangeInfo
    ) : base(
        CreateSweeps(
            sweepData.ToDictionary(kvp => kvp.Key, kvp => (IReadOnlyDictionary<FieldName, Radar.Field>)kvp.Value),
            elevationAngles,
            startAzimuths,
            sweepRangeInfo.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
        ),
        (datetime, height, latitude, longitude)
    )
    {


    }


    private static Dictionary<int, NexradLevel2Sweep> CreateSweeps(
        IReadOnlyDictionary<int, IReadOnlyDictionary<FieldName, Radar.Field>> sweepData,
        IReadOnlyList<double> elevationAngles,
        IReadOnlyList<double> startAzimuths,
        IReadOnlyDictionary<int, (short rangeStart, short rangeScale)> sweepRangeInfo
    )
    {
        return sweepData.ToDictionary(
            kvp => kvp.Key,
            kvp =>
            {
                var scanIndex = kvp.Key;
                var (rangeStart, rangeScale) = sweepRangeInfo[kvp.Key];
                var elevationAngle = elevationAngles[scanIndex];
                var startAzimuth = startAzimuths[scanIndex];
                return new NexradLevel2Sweep(
                    kvp.Value,
                    scanIndex,
                    startAzimuth,
                    elevationAngle,
                    rangeScale,
                    rangeStart
                );
            }
        );
    }

    // Convenience properties for backward compatibility
    // public DateTime DateTime => Attributes.datetime;
    public IReadOnlyList<double> ElevationAngles => Data.Values.Select(s => s.ElevationAngle).ToList().AsReadOnly();


}
