using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace NexradSharp;
public static class StructSize
{
    public static int Of<T>() where T : unmanaged => Unsafe.SizeOf<T>();
}


public interface IMessageHeader
{
    public SweepStatus GetStatus();
}

public enum SweepStatus : byte
{
    StartOfNewElevation = 0,
    IntermediateRadial = 1,
    EndOfElevation = 2,
    StartOfNewVolume = 3,
    EndOfVolume = 4,
    StartOfNewElevationLastElevationInVCP = 5,
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct MessageHeader(
    ushort size,
    byte channels,
    byte type,
    ushort sequenceId,
    ushort date,
    uint milliseconds,
    ushort segments,
    ushort segmentNumber
)
{
    public readonly ushort Size = size;
    public readonly byte Channels = channels;
    public readonly byte Type = type;
    public readonly ushort SequenceId = sequenceId;
    public readonly ushort Date = date;
    public readonly uint Milliseconds = milliseconds;
    public readonly ushort Segments = segments;
    public readonly ushort SegmentNumber = segmentNumber;
    public static int SizeOf => StructSize.Of<MessageHeader>();
    public MessageHeader(Span<byte> span) : this(
        BinaryPrimitives.ReadUInt16BigEndian(span.Slice(0, 2)), //  Size
        span[2], //                                                  Channels
        span[3], // Type
        BinaryPrimitives.ReadUInt16BigEndian(span.Slice(4, 2)), //  SequenceId
        BinaryPrimitives.ReadUInt16BigEndian(span.Slice(6, 2)), //  Date
        BinaryPrimitives.ReadUInt32BigEndian(span.Slice(8, 4)), //  Milliseconds
        BinaryPrimitives.ReadUInt16BigEndian(span.Slice(10, 2)), // Segments
        BinaryPrimitives.ReadUInt16BigEndian(span.Slice(12, 2)) //  SegmentNumber
    )
    { }
    public MessageHeader(byte[] bytes) : this(bytes.AsSpan()) { }
    public static MessageHeader Read(BinaryReader reader) => new(reader.ReadBytes(SizeOf));
}




[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct MessageHeaderWithMetadata(
    MessageHeader header,
    int recordNumber,
    long filePosition
)
{
    public readonly MessageHeader Header = header;
    public readonly int RecordNumber = recordNumber;
    public readonly long FilePosition = filePosition;

    public static int SizeOf => StructSize.Of<MessageHeaderWithMetadata>();
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct Message31Header : IMessageHeader
{
    [MarshalAs(UnmanagedType.ByValArray, SizeConst = 4)]
    private readonly byte[] _idBytes;
    public uint CollectMilliseconds;
    public ushort CollectDate;
    public ushort AzimuthNumber;
    public float AzimuthAngle;
    public byte CompressFlag;
    public byte Spare0;
    public ushort RadialLength;
    public byte AzimuthResolution;
    public byte RadialStatus;
    public byte ElevationNumber;
    public byte CutSector;
    public float ElevationAngle;
    public byte RadialBlanking;
    public sbyte AzimuthMode;
    public ushort BlockCount;
    [MarshalAs(UnmanagedType.ByValArray, SizeConst = 10)]
    private readonly uint[] _blockPointers;

    public readonly string Id
    {
        get
        {
            if (_idBytes == null) return string.Empty;
            return Encoding.ASCII.GetString(_idBytes, 0, 4).TrimEnd('\0');
        }
    }

    public readonly uint[] BlockPointers
    {
        get
        {
            if (_blockPointers == null) return new uint[10];
            var result = new uint[10];
            Array.Copy(_blockPointers, result, Math.Min(10, _blockPointers.Length));
            return result;
        }
    }

    public Message31Header(
        string id,
        uint collectMilliseconds,
        ushort collectDate,
        ushort azimuthNumber,
        float azimuthAngle,
        byte compressFlag,
        byte spare0,
        ushort radialLength,
        byte azimuthResolution,
        byte radialStatus,
        byte elevationNumber,
        byte cutSector,
        float elevationAngle,
        byte radialBlanking,
        sbyte azimuthMode,
        ushort blockCount,
        uint[] blockPointers
    )
    {
        _idBytes = new byte[4];
        var idBytes = Encoding.ASCII.GetBytes(id.PadRight(4, '\0'));
        Array.Copy(idBytes, _idBytes, Math.Min(4, idBytes.Length));

        _blockPointers = new uint[10];
        if (blockPointers != null)
        {
            Array.Copy(blockPointers, _blockPointers, Math.Min(10, blockPointers.Length));
        }

        CollectMilliseconds = collectMilliseconds;
        CollectDate = collectDate;
        AzimuthNumber = azimuthNumber;
        AzimuthAngle = azimuthAngle;
        CompressFlag = compressFlag;
        Spare0 = spare0;
        RadialLength = radialLength;
        AzimuthResolution = azimuthResolution;
        RadialStatus = radialStatus;
        ElevationNumber = elevationNumber;
        CutSector = cutSector;
        ElevationAngle = elevationAngle;
        RadialBlanking = radialBlanking;
        AzimuthMode = azimuthMode;
        BlockCount = blockCount;
    }

    public readonly SweepStatus GetStatus() => (SweepStatus)RadialStatus;

    public static int SizeOf => 72;

    /// <summary>
    /// Reads a Message31Header from a BinaryReader (big-endian format).
    /// </summary>
    public static Message31Header Read(BinaryReader reader)
    {
        var bytes = reader.ReadBytes(SizeOf);
        return new Message31Header(bytes);
    }

    /// <summary>
    /// Constructs a Message31Header from a byte array (big-endian format).
    /// </summary>
    public Message31Header(byte[] bytes) : this(bytes.AsSpan())
    {
    }

    /// <summary>
    /// Constructs a Message31Header from a Span<byte> (big-endian format).
    /// </summary>
    public Message31Header(Span<byte> span)
    {
        if (span.Length < SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));

        int offset = 0;

        // Read ID (4 bytes ASCII)
        _idBytes = new byte[4];
        span.Slice(offset, 4).CopyTo(_idBytes);
        offset += 4;

        CollectMilliseconds = BinaryPrimitives.ReadUInt32BigEndian(span.Slice(offset, 4));
        offset += 4;
        CollectDate = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        AzimuthNumber = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        AzimuthAngle = BinaryPrimitives.ReadSingleBigEndian(span.Slice(offset, 4));
        offset += 4;
        CompressFlag = span[offset++];
        Spare0 = span[offset++];
        RadialLength = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        AzimuthResolution = span[offset++];
        RadialStatus = span[offset++];
        ElevationNumber = span[offset++];
        CutSector = span[offset++];
        ElevationAngle = BinaryPrimitives.ReadSingleBigEndian(span.Slice(offset, 4));
        offset += 4;
        RadialBlanking = span[offset++];
        AzimuthMode = (sbyte)span[offset++];
        BlockCount = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;

        // Read block pointers (10 * 4 bytes = 40 bytes)
        _blockPointers = new uint[10];
        for (int i = 0; i < 10; i++)
        {
            _blockPointers[i] = BinaryPrimitives.ReadUInt32BigEndian(span.Slice(offset, 4));
            offset += 4;
        }
    }
}

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public readonly struct Message1Header : IMessageHeader
{
    [FieldOffset(0)] public readonly uint CollectMilliseconds;
    [FieldOffset(4)] public readonly ushort CollectDate;
    [FieldOffset(6)] public readonly short UnambiguousRange;
    [FieldOffset(8)] public readonly ushort AzimuthAngle;
    [FieldOffset(10)] public readonly ushort AzimuthNumber;
    [FieldOffset(12)] public readonly ushort RadialStatus;
    [FieldOffset(14)] public readonly ushort ElevationAngle;
    [FieldOffset(16)] public readonly ushort ElevationNumber;
    [FieldOffset(18)] public readonly ushort SurveillanceRangeFirst;
    [FieldOffset(20)] public readonly ushort DopplerRangeFirst;
    [FieldOffset(22)] public readonly ushort SurveillanceRangeStep;
    [FieldOffset(24)] public readonly ushort DopplerRangeStep;
    [FieldOffset(26)] public readonly ushort SurveillanceNumberOfBins;
    [FieldOffset(28)] public readonly ushort DopplerNumberOfBins;
    [FieldOffset(30)] public readonly ushort CutSectorNumber;
    [FieldOffset(32)] public readonly float CalibrationConstant;
    [FieldOffset(36)] public readonly ushort SurveillancePointer;
    [FieldOffset(38)] public readonly ushort VelocityPointer;
    [FieldOffset(40)] public readonly ushort WidthPointer;
    [FieldOffset(42)] public readonly ushort DopplerResolution;
    [FieldOffset(44)] public readonly ushort VolumeCoveragePattern;
    // spare_1: 8 bytes at offset 46-53 (skipped)
    // spare_2: 2 bytes at offset 54-55 (skipped)
    // spare_3: 2 bytes at offset 56-57 (skipped)
    // spare_4: 2 bytes at offset 58-59 (skipped)
    [FieldOffset(60)] public readonly short NyquistVelocity;
    [FieldOffset(62)] public readonly short AtmosphericAttenuation;
    [FieldOffset(64)] public readonly short Threshold;
    [FieldOffset(66)] public readonly ushort SpotBlankStatus;
    // spare_5: 32 bytes at offset 68-99 (skipped)

    public Message1Header(
        uint collectMilliseconds,
        ushort collectDate,
        short unambiguousRange,
        ushort azimuthAngle,
        ushort azimuthNumber,
        ushort radialStatus,
        ushort elevationAngle,
        ushort elevationNumber,
        ushort surveillanceRangeFirst,
        ushort dopplerRangeFirst,
        ushort surveillanceRangeStep,
        ushort dopplerRangeStep,
        ushort surveillanceNumberOfBins,
        ushort dopplerNumberOfBins,
        ushort cutSectorNumber,
        float calibrationConstant,
        ushort surveillancePointer,
        ushort velocityPointer,
        ushort widthPointer,
        ushort dopplerResolution,
        ushort volumeCoveragePattern,
        short nyquistVelocity,
        short atmosphericAttenuation,
        short threshold,
        ushort spotBlankStatus
    )
    {
        CollectMilliseconds = collectMilliseconds;
        CollectDate = collectDate;
        UnambiguousRange = unambiguousRange;
        AzimuthAngle = azimuthAngle;
        AzimuthNumber = azimuthNumber;
        RadialStatus = radialStatus;
        ElevationAngle = elevationAngle;
        ElevationNumber = elevationNumber;
        SurveillanceRangeFirst = surveillanceRangeFirst;
        DopplerRangeFirst = dopplerRangeFirst;
        SurveillanceRangeStep = surveillanceRangeStep;
        DopplerRangeStep = dopplerRangeStep;
        SurveillanceNumberOfBins = surveillanceNumberOfBins;
        DopplerNumberOfBins = dopplerNumberOfBins;
        CutSectorNumber = cutSectorNumber;
        CalibrationConstant = calibrationConstant;
        SurveillancePointer = surveillancePointer;
        VelocityPointer = velocityPointer;
        WidthPointer = widthPointer;
        DopplerResolution = dopplerResolution;
        VolumeCoveragePattern = volumeCoveragePattern;
        NyquistVelocity = nyquistVelocity;
        AtmosphericAttenuation = atmosphericAttenuation;
        Threshold = threshold;
        SpotBlankStatus = spotBlankStatus;
    }

    public readonly SweepStatus GetStatus() => (SweepStatus)RadialStatus;

    public static int SizeOf => 100;

    /// <summary>
    /// Reads a Message1Header from a BinaryReader (big-endian format).
    /// </summary>
    public static Message1Header Read(BinaryReader reader)
    {
        var bytes = reader.ReadBytes(SizeOf);
        return new Message1Header(bytes);
    }

    /// <summary>
    /// Constructs a Message1Header from a byte array (big-endian format).
    /// </summary>
    public Message1Header(byte[] bytes) : this(bytes.AsSpan())
    {
    }

    /// <summary>
    /// Constructs a Message1Header from a Span<byte> (big-endian format).
    /// </summary>
    public Message1Header(Span<byte> span)
    {
        if (span.Length < SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));

        int offset = 0;

        CollectMilliseconds = BinaryPrimitives.ReadUInt32BigEndian(span.Slice(offset, 4));
        offset += 4;
        CollectDate = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        UnambiguousRange = (short)BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        AzimuthAngle = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        AzimuthNumber = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        RadialStatus = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        ElevationAngle = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        ElevationNumber = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        SurveillanceRangeFirst = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        DopplerRangeFirst = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        SurveillanceRangeStep = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        DopplerRangeStep = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        SurveillanceNumberOfBins = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        DopplerNumberOfBins = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        CutSectorNumber = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        CalibrationConstant = BinaryPrimitives.ReadSingleBigEndian(span.Slice(offset, 4));
        offset += 4;
        SurveillancePointer = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        VelocityPointer = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        WidthPointer = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        DopplerResolution = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        VolumeCoveragePattern = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        // Skip spare_1 (8 bytes), spare_2 (2 bytes), spare_3 (2 bytes), spare_4 (2 bytes) = 14 bytes total
        offset += 14;
        NyquistVelocity = (short)BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        AtmosphericAttenuation = (short)BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        Threshold = (short)BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        SpotBlankStatus = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        // Skip spare_5 (32 bytes) - not needed, we're done
    }
}

public record VolumeBlock(
    ushort LastRecordTimeUpdatePointer,
    byte VersionMajor,
    byte VersionMinor,
    float Latitude,
    float Longitude,
    short Height,
    ushort FeedhornHeight,
    float ReflectivityCalibration,
    float PowerHorizontal,
    float PowerVertical,
    float DifferentialReflectivityCalibration,
    float InitialPhase,
    ushort VolumeCoveragePattern
);

public record ElevationBlock(
    ushort LastRecordTimeUpdatePointer,
    short AtmosphericAttenuation,
    float ReflectivityCalibration
);

public record RadialBlock(
    ushort LastRecordTimeUpdatePointer,
    short UnambiguousRange,
    float NoiseHorizontal,
    float NoiseVertical,
    short NyquistVelocity
);

/// <summary>
/// Union type for data blocks stored in Message31DataHeader.
/// </summary>
public abstract record DataBlock;

/// <summary>
/// Constant data blocks (VOL, ELV, RAD).
/// </summary>
public sealed record ConstantDataBlock(VolumeBlock? Volume, ElevationBlock? Elevation, RadialBlock? Radial) : DataBlock
{
    public static ConstantDataBlock FromVolume(VolumeBlock volume) => new(volume, null, null);
    public static ConstantDataBlock FromElevation(ElevationBlock elevation) => new(null, elevation, null);
    public static ConstantDataBlock FromRadial(RadialBlock radial) => new(null, null, radial);
}

/// <summary>
/// Variable data blocks (REF, VEL, SW, ZDR, PHI, RHO, CFP).
/// </summary>
public sealed record VariableDataBlock(VariableBlock Variable) : DataBlock;

public record VariableBlock(
    uint Reserved,
    ushort NumberOfGates,
    short FirstGate,
    short GateSpacing,
    short Threshold,
    short SignalToNoiseRatioThreshold,
    byte Flags,
    byte WordSize,
    float Scale,
    float Offset,
    long DataOffset
)
{
    /// <summary>
    /// Size of the VariableBlock structure when read from binary data (24 bytes).
    /// Note: DataOffset is not part of the binary structure, it's calculated separately.
    /// </summary>
    public static int SizeOf => 24;
    /// <summary>
    /// Constructs a VariableBlock from a Span<byte> (big-endian format).
    /// </summary>
    /// <param name="span">The span containing the VariableBlock data</param>
    /// <param name="dataOffset">The data offset (calculated separately, not part of the binary structure)</param>
    public VariableBlock(Span<byte> span, long dataOffset) : this(
        Reserved: BinaryPrimitives.ReadUInt32BigEndian(span[..4]),
        NumberOfGates: BinaryPrimitives.ReadUInt16BigEndian(span[4..6]),
        FirstGate: (short)BinaryPrimitives.ReadUInt16BigEndian(span[6..8]),
        GateSpacing: (short)BinaryPrimitives.ReadUInt16BigEndian(span[8..10]),
        Threshold: (short)BinaryPrimitives.ReadUInt16BigEndian(span.Slice(10, 2)),
        SignalToNoiseRatioThreshold: (short)BinaryPrimitives.ReadUInt16BigEndian(span[12..14]),
        Flags: span[14],
        WordSize: span[15],
        Scale: BinaryPrimitives.ReadSingleBigEndian(span[16..20]),
        Offset: BinaryPrimitives.ReadSingleBigEndian(span[20..24]),
        DataOffset: dataOffset
    )
    {
        if (span.Length < SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));
    }
    public VariableBlock(Message1Header msg1, uint reserved, short threshold, short signalToNoiseRatioThreshold, byte flags, byte wordSize, float scale, float offset, int pointer) : this(
        Reserved: reserved,
        NumberOfGates: msg1.SurveillanceNumberOfBins,
        FirstGate: (short)msg1.SurveillanceRangeFirst,
        GateSpacing: (short)msg1.SurveillanceRangeStep,
        Threshold: threshold,
        SignalToNoiseRatioThreshold: signalToNoiseRatioThreshold,
        Flags: flags,
        WordSize: wordSize,
        Scale: scale,
        Offset: offset,
        DataOffset: pointer + MessageHeader.SizeOf + 12
    )
    { }
    public VariableBlock(Message1Header msg1, float scale, float offset, int pointer) : this(
        Reserved: 0,
        NumberOfGates: msg1.SurveillanceNumberOfBins,
        FirstGate: (short)msg1.SurveillanceRangeFirst,
        GateSpacing: (short)msg1.SurveillanceRangeStep,
        Threshold: 0,
        SignalToNoiseRatioThreshold: 0,
        Flags: 0,
        WordSize: 8,
        Scale: scale,
        Offset: offset,
        DataOffset: pointer + MessageHeader.SizeOf + 12
    )
    { }


    /// <summary>
    /// Constructs a VariableBlock from a byte array (big-endian format).
    /// </summary>
    /// <param name="bytes">The byte array containing the VariableBlock data</param>
    /// <param name="dataOffset">The data offset (calculated separately, not part of the binary structure)</param>
    public VariableBlock(byte[] bytes, long dataOffset) : this(bytes.AsSpan(), dataOffset)
    {
    }
    /// <summary>
    /// Reads a VariableBlock from a BinaryReader (big-endian format).
    /// Python equivalent: Parse variable data block (REF, VEL, SW, ZDR, PHI, RHO, CFP).
    /// </summary>
    /// <param name="reader">The BinaryReader to read from</param>
    /// <param name="dataOffset">The data offset (calculated separately, not read from stream)</param>
    /// <returns>A new VariableBlock instance</returns>
    public static VariableBlock Read(BinaryReader reader, long dataOffset)
    {
        var bytes = reader.ReadBytes(SizeOf);
        return new VariableBlock(bytes, dataOffset);
    }
}

public record SweepData(
    int RecordNumber,
    long FilePosition,
    Message31Header Message31Header,
    byte MessageType,
    Dictionary<string, DataBlock> Message31DataHeader,
    int? RecordEnd = null,
    List<int>? IntermediateRecords = null
);
/// <summary>
/// Metadata extracted from NEXRAD file headers without reading sweep data.
/// </summary>
public record ScanMetadata
{
    public double Elevation { get; init; }
    public List<string> Quantities { get; init; } = [];
    public double Latitude { get; init; }
    public double Longitude { get; init; }
    public double Height { get; init; }
    public DateTime Timestamp { get; init; }
}

