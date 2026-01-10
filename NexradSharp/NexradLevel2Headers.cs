
using System.Runtime.InteropServices;
using System.Text;


namespace NexradSharp;

using static System.Buffers.Binary.BinaryPrimitives;


public interface IMessageHeader { public SweepStatus GetStatus(); }


[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct MessageHeader(
    ushort size,
    byte channels,
    MessageType type,
    ushort sequenceId,
    ushort date,
    uint milliseconds,
    ushort segments,
    ushort segmentNumber
)
{
    public readonly ushort Size = size;
    public readonly byte Channels = channels;
    public readonly MessageType Type = type;
    public readonly ushort SequenceId = sequenceId;
    public readonly ushort Date = date;
    public readonly uint Milliseconds = milliseconds;
    public readonly ushort Segments = segments;
    public readonly ushort SegmentNumber = segmentNumber;
    public const int SizeOf = 16;
    public MessageHeader(Span<byte> span) : this(
        ReadUInt16BigEndian(span[..2]),    //  Size
        span[2],                           // Channels
        (MessageType)span[3],              // Type
        ReadUInt16BigEndian(span[4..6]),   // SequenceId
        ReadUInt16BigEndian(span[6..8]),   // Date
        ReadUInt32BigEndian(span[8..12]),  // Milliseconds
        ReadUInt16BigEndian(span[10..12]), // Segments
        ReadUInt16BigEndian(span[12..14])  // SegmentNumber
    )
    { }
    public MessageHeader(byte[] bytes) : this(bytes.AsSpan()) { }
    public static MessageHeader Read(BinaryReader reader) => new(reader.ReadBytes(SizeOf));
    public MessageHeaderWithMetadata With(int recordNumber, long filePosition) => new(this, recordNumber, filePosition);
}


[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct MessageHeaderWithMetadata(MessageHeader header, int recordNumber, long filePosition)
{
    public readonly MessageHeader Header = header;
    public readonly int RecordNumber = recordNumber;
    public readonly long FilePosition = filePosition;
    public const int SizeOf = MessageHeader.SizeOf + 4 + 8;

    public MessageHeaderWithMetadata(Span<byte> span) : this(
        header: new MessageHeader(span[..MessageHeader.SizeOf]),
        recordNumber: ReadInt32BigEndian(span[MessageHeader.SizeOf..(MessageHeader.SizeOf + 4)]),
        filePosition: ReadInt64BigEndian(span[(MessageHeader.SizeOf + 4)..(MessageHeader.SizeOf + 12)])
    )
    { }
    public MessageHeaderWithMetadata(byte[] bytes) : this(bytes.AsSpan()) { }
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct RadarDataHeader : IMessageHeader
{
    // [MarshalAs(UnmanagedType.ByValArray, SizeConst = 4)]
    // private readonly byte[] _idBytes;
    public string Id;
    public uint CollectMilliseconds;
    public ushort CollectDate;
    public ushort AzimuthNumber;
    public float AzimuthAngle;
    public byte CompressFlag;
    public byte Spare0;
    public ushort RadialLength;
    public byte AzimuthResolution;
    public SweepStatus RadialStatus;
    public byte ElevationNumber;
    public byte CutSector;
    public float ElevationAngle;
    public byte RadialBlanking;
    public sbyte AzimuthMode;
    public ushort BlockCount;
    [MarshalAs(UnmanagedType.ByValArray, SizeConst = 10)]
    private readonly uint[] _blockPointers;
    public const int SizeOf = 72;

    // public readonly string Id
    // {
    //     get
    //     {
    //         if (_idBytes == null) return string.Empty;
    //         return Encoding.ASCII.GetString(_idBytes, 0, 4).TrimEnd('\0');
    //     }
    // }

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


    public readonly SweepStatus GetStatus() => RadialStatus;



    /// <summary>
    /// Reads a RadarDataHeader from a BinaryReader (big-endian format).
    /// </summary>
    public static RadarDataHeader Read(BinaryReader reader) => new(reader.ReadBytes(SizeOf));


    /// <summary>
    /// Constructs a RadarDataHeader from a byte array (big-endian format).
    /// </summary>
    public RadarDataHeader(byte[] bytes) : this(bytes.AsSpan()) { }

    /// <summary>
    /// Constructs a RadarDataHeader from a Span<byte> (big-endian format).
    /// </summary>
    public RadarDataHeader(Span<byte> span)
    {
        if (span.Length != SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));
        Id = Encoding.ASCII.GetString(span[0..4]).TrimEnd('\0');
        CollectMilliseconds = ReadUInt32BigEndian(span[4..8]);
        CollectDate = ReadUInt16BigEndian(span[8..10]);
        AzimuthNumber = ReadUInt16BigEndian(span[10..12]);
        AzimuthAngle = ReadSingleBigEndian(span[12..16]);
        CompressFlag = span[16];
        Spare0 = span[17];
        RadialLength = ReadUInt16BigEndian(span[18..20]);
        AzimuthResolution = span[20];
        RadialStatus = (SweepStatus)span[21];
        ElevationNumber = span[22];
        CutSector = span[23];
        ElevationAngle = ReadSingleBigEndian(span[24..28]);
        RadialBlanking = span[28];
        AzimuthMode = (sbyte)span[29];
        BlockCount = ReadUInt16BigEndian(span[30..32]);

        // Read block pointers (10 * 4 bytes = 40 bytes)
        _blockPointers = new uint[10];
        var start = 32;
        var stop = start + 4;
        for (int i = 0; i < 10; i++)
        {
            _blockPointers[i] = ReadUInt32BigEndian(span[start..stop]);
            start = stop;
            stop += 4;
        }
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
)
{
    /// <summary>
    /// Size of the VolumeBlock structure when read from binary data (40 bytes including 2-byte spare).
    /// </summary>
    public const int SizeOf = 40;

    /// <summary>
    /// Constructs a VolumeBlock from a Span<byte> (big-endian format).
    /// </summary>
    /// <param name="span">The span containing the VolumeBlock data</param>
    public VolumeBlock(Span<byte> span) : this(
        LastRecordTimeUpdatePointer: ReadUInt16BigEndian(span[0..2]),
        VersionMajor: span[2],
        VersionMinor: span[3],
        Latitude: ReadSingleBigEndian(span[4..8]),
        Longitude: ReadSingleBigEndian(span[8..12]),
        Height: (short)ReadUInt16BigEndian(span[12..14]), // SINT2
        FeedhornHeight: ReadUInt16BigEndian(span[14..16]),
        ReflectivityCalibration: ReadSingleBigEndian(span[16..20]),
        PowerHorizontal: ReadSingleBigEndian(span[20..24]),
        PowerVertical: ReadSingleBigEndian(span[24..28]),
        DifferentialReflectivityCalibration: ReadSingleBigEndian(span[28..32]),
        InitialPhase: ReadSingleBigEndian(span[32..36]),
        VolumeCoveragePattern: ReadUInt16BigEndian(span[36..38])
    // Note: bytes 38-40 are spare and not read
    )
    {
        if (span.Length < SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));
    }

    /// <summary>
    /// Constructs a VolumeBlock from a byte array (big-endian format).
    /// </summary>
    /// <param name="bytes">The byte array containing the VolumeBlock data</param>
    public VolumeBlock(byte[] bytes) : this(bytes.AsSpan()) { }
    public VolumeBlock(BinaryReader reader) : this(reader.ReadBytes(SizeOf)) { }
}

public record ElevationBlock(
    ushort LastRecordTimeUpdatePointer,
    short AtmosphericAttenuation,
    float ReflectivityCalibration
)
{
    /// <summary>
    /// Size of the ElevationBlock structure when read from binary data (8 bytes).
    /// </summary>
    public const int SizeOf = 8;

    /// <summary>
    /// Constructs an ElevationBlock from a Span<byte> (big-endian format).
    /// </summary>
    /// <param name="span">The span containing the ElevationBlock data</param>
    public ElevationBlock(Span<byte> span) : this(
        LastRecordTimeUpdatePointer: ReadUInt16BigEndian(span[0..2]),
        AtmosphericAttenuation: (short)ReadUInt16BigEndian(span[2..4]), // SINT2
        ReflectivityCalibration: ReadSingleBigEndian(span[4..8])
    )
    {
        if (span.Length < SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));
    }

    /// <summary>
    /// Constructs an ElevationBlock from a byte array (big-endian format).
    /// </summary>
    /// <param name="bytes">The byte array containing the ElevationBlock data</param>
    public ElevationBlock(byte[] bytes) : this(bytes.AsSpan()) { }
    public ElevationBlock(BinaryReader reader) : this(reader.ReadBytes(SizeOf)) { }
}

public record RadialBlock(
    ushort LastRecordTimeUpdatePointer,
    short UnambiguousRange,
    float NoiseHorizontal,
    float NoiseVertical,
    short NyquistVelocity
)
{
    /// <summary>
    /// Size of the RadialBlock structure when read from binary data (16 bytes including 2-byte spare).
    /// </summary>
    public const int SizeOf = 16;

    /// <summary>
    /// Constructs a RadialBlock from a Span<byte> (big-endian format).
    /// </summary>
    /// <param name="span">The span containing the RadialBlock data</param>
    public RadialBlock(Span<byte> span) : this(
        LastRecordTimeUpdatePointer: ReadUInt16BigEndian(span[0..2]),
        UnambiguousRange: (short)ReadUInt16BigEndian(span[2..4]), // SINT2
        NoiseHorizontal: ReadSingleBigEndian(span[4..8]),
        NoiseVertical: ReadSingleBigEndian(span[8..12]),
        NyquistVelocity: (short)ReadUInt16BigEndian(span[12..14]) // SINT2
    )
    {
        if (span.Length != SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));
    }

    /// <summary>
    /// Constructs a RadialBlock from a byte array (big-endian format).
    /// </summary>
    /// <param name="bytes">The byte array containing the RadialBlock data</param>
    public RadialBlock(byte[] bytes) : this(bytes.AsSpan()) { }
    public RadialBlock(BinaryReader reader) : this(reader.ReadBytes(SizeOf)) { }
}

/// <summary>
/// Union type for data blocks stored in Message31DataHeader.
/// </summary>
public abstract record DataBlock;


/// <summary>
/// Constant data blocks (VOL, ELV, RAD).
// /// </summary>
// public sealed record ConstantDataBlock(VolumeBlock? Volume, ElevationBlock? Elevation, RadialBlock? Radial) : DataBlock
// {
//     public static ConstantDataBlock FromVolume(VolumeBlock volume) => new(volume, null, null);
//     public static ConstantDataBlock FromElevation(ElevationBlock elevation) => new(null, elevation, null);
//     public static ConstantDataBlock FromRadial(RadialBlock radial) => new(null, null, radial);
// }
public sealed record ConstantBlock(VolumeBlock Volume, ElevationBlock Elevation, RadialBlock Radial) : DataBlock
{
    /// <summary>
    /// Constructs a ConstantBlock by reading all three blocks (VOL, ELV, RAD) from a BinaryReader.
    /// The reader should be positioned at the start of the VOL block's DATA_BLOCK_HEADER.
    /// </summary>
    public ConstantBlock(BinaryReader reader, (uint vol, uint elv, uint rad) positions) : this(ReadBytes(reader, positions))
    { }
    private const int CTM_HEADER_OFFSET = 12;
#if DEBUG
    private static readonly byte[] VOL_HEADER = Encoding.ASCII.GetBytes("RVOL");
    private static readonly byte[] ELV_HEADER = Encoding.ASCII.GetBytes("RELV");
    private static readonly byte[] RAD_HEADER = Encoding.ASCII.GetBytes("RRAD");
    private const int MESSAGE_HEADER_OFFSET = CTM_HEADER_OFFSET + MessageHeader.SizeOf;
#else
    private const int MESSAGE_HEADER_OFFSET = CTM_HEADER_OFFSET + MessageHeader.SizeOf + 4; // 4 bytes for the DATA_BLOCK_HEADER
#endif

    private static byte[] ReadBytes(BinaryReader reader, (uint vol, uint elv, uint rad) positions)
    {
        var bytes = new byte[SizeOf];
        // Read VOL block (index 0)
        reader.BaseStream.Position = positions.vol + MESSAGE_HEADER_OFFSET;
#if DEBUG
        if (!VOL_HEADER.SequenceEqual(reader.ReadBytes(4))) throw new InvalidOperationException($"Expected RVOL header");
#endif
        reader.ReadBytes(VolumeBlock.SizeOf).CopyTo(bytes, 0);

        // Read ELV block (index 1)
        reader.BaseStream.Position = positions.elv + MESSAGE_HEADER_OFFSET;
#if DEBUG
        if (!ELV_HEADER.SequenceEqual(reader.ReadBytes(4))) throw new InvalidOperationException($"Expected RELV header");
#endif
        reader.ReadBytes(ElevationBlock.SizeOf).CopyTo(bytes, VolumeBlock.SizeOf);

        // Read RAD block (index 2)
        reader.BaseStream.Position = positions.rad + MESSAGE_HEADER_OFFSET;
#if DEBUG
        if (!RAD_HEADER.SequenceEqual(reader.ReadBytes(4))) throw new InvalidOperationException($"Expected RRAD header");
#endif

        reader.ReadBytes(RadialBlock.SizeOf).CopyTo(bytes, VolumeBlock.SizeOf + ElevationBlock.SizeOf);

        return bytes;
    }

    /// <summary>
    /// Constructs a ConstantBlock from a contiguous byte array containing VOL, ELV, and RAD blocks.
    /// </summary>
    public ConstantBlock(byte[] bytes) : this(bytes.AsSpan())
    { }

    /// <summary>
    /// Constructs a ConstantBlock from a contiguous Span containing VOL, ELV, and RAD blocks.
    /// </summary>
    public ConstantBlock(Span<byte> span) : this(
        Volume: new VolumeBlock(span[..VolumeBlock.SizeOf]),
        Elevation: new ElevationBlock(span[VolumeBlock.SizeOf..(VolumeBlock.SizeOf + ElevationBlock.SizeOf)]),
        Radial: new RadialBlock(span[(VolumeBlock.SizeOf + ElevationBlock.SizeOf)..SizeOf])
    )
    {
        if (span.Length < SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));
    }

    public static int SizeOf => VolumeBlock.SizeOf + ElevationBlock.SizeOf + RadialBlock.SizeOf;
}

/// <summary>
/// Variable data blocks (REF, VEL, SW, ZDR, PHI, RHO, CFP).
/// </summary>
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
) : DataBlock
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
        Reserved: ReadUInt32BigEndian(span[..4]),
        NumberOfGates: ReadUInt16BigEndian(span[4..6]),
        FirstGate: (short)ReadUInt16BigEndian(span[6..8]),
        GateSpacing: (short)ReadUInt16BigEndian(span[8..10]),
        Threshold: (short)ReadUInt16BigEndian(span.Slice(10, 2)),
        SignalToNoiseRatioThreshold: (short)ReadUInt16BigEndian(span[12..14]),
        Flags: span[14],
        WordSize: span[15],
        Scale: ReadSingleBigEndian(span[16..20]),
        Offset: ReadSingleBigEndian(span[20..24]),
        DataOffset: dataOffset
    )
    {
        if (span.Length != SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));
    }


    /// <summary>
    /// Constructs a VariableBlock from a byte array (big-endian format).
    /// </summary>
    /// <param name="bytes">The byte array containing the VariableBlock data</param>
    /// <param name="dataOffset">The data offset (calculated separately, not part of the binary structure)</param>
    public VariableBlock(byte[] bytes, long dataOffset) : this(bytes.AsSpan(), dataOffset) { }
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
    RadarDataHeader RadarDataHeader,
    MessageType MessageType,
    ConstantBlock ConstantBlock,
    Dictionary<DataName, DataBlock> Message31DataHeader,
    int? RecordEnd = null,
    List<int>? IntermediateRecords = null
);
