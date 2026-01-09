using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;

namespace NexradSharp;

public interface IMessageHeader
{
    public SweepStatus GetStatus();
}

public enum SweepStatus : byte
{
    NEW_ELEVATION = 0,
    INTERMEDIATE_RADIAL = 1,
    END_ELEVATION = 2,
    NEW_VOLUME = 3,
    END_VOLUME = 4,
    NEW_ELEVATION_LAST_ELEVATION_IN_VCP = 5,
}
public enum BlockType : byte
{
    /// <summary>
    /// Constant block (VOL, ELV, RAD).
    /// </summary>
    CONSTANT = (byte)'R',
    /// <summary>
    /// Variable block (REF, VEL, SW, ZDR, PHI, RHO, CFP).
    /// </summary>
    VARIABLE = (byte)'D',
}
public enum DataName : byte
{
    VOL,
    ELV,
    RAD,
    REF,
    VEL,
    SW,
    ZDR,
    PHI,
    RHO,
    CFP,
}

/// <summary>
/// NEXRAD message types.
/// 3.2.2 Operating Procedures 
// The data messages to be transferred between the RDA and the RPG are listed in Table I.  The data 
// messages will be exchanged after a successful session is established.  A message header of format 
// specified in Table II is attached to each message transmitted across the link. 

// Table I   Data Message Types  
// Type Description Source Recipient Format 
// 1 Digital Radar Data  RDA RPG Table  III 
// 2* RDA Status Data  RDA RPG/RMS Table  IV 
// 3* Performance/Maintenance Data RDA RPG/RMS Table  V 
// 4 Console Message RDA RPG/RMS Table  VI 
// 5* Volume Coverage Pattern RDA RPG Table XI 
// 6 RDA Control Commands RPG RDA Table  X 
// 7 Volume Coverage Pattern RPG RDA Table  XI 
// 8 Clutter Censor Zones RPG RDA Table  XII 
// 9 Request for Data RPG RDA Table  XIII 
// 10 Console Message RPG RDA/RMS Table  VI 
// 11 Loop Back Test RDA RPG Table  VIII 
// 12 Loop Back Test RPG RDA Table  VIII 
// 13*  Clutter Filter Bypass Map RDA RPG Table  IX 
// 14 Spare N/A N/A N/A 
// 15* Clutter Filter Map RDA RPG Table XIV 
// 16 Reserved/FAA RMS Only N/A N/A N/A 
// 17 Reserved/FAA RMS Only N/A N/A N/A 
// 18* RDA Adaptation Data RDA RPG/RMS Table XV 
// 20 Reserved N/A N/A N/A 
// 21 Reserved N/A N/A N/A 
// 22 Reserved N/A N/A N/A 
// 23 Reserved N/A N/A N/A 
// 24 Reserved/FAA RMS only N/A N/A N/A 
// 25 Reserved/FAA RMS only N/A N/A N/A 
// 26 Reserved/FAA RMS only N/A N/A N/A 
// 31 Digital Radar Data Generic 
// Format 
// RDA RPG Table XVII 

// * = metadata 
/// </summary>
public enum MessageType : byte
{
    /// <summary>
    /// Metadata message type.
    /// </summary>
    Metadata = 0,
    /// <summary>
    /// Digital Radar Data (Legacy Format)
    /// </summary>
    LEGACY_FORMAT = 1,
    /// <summary>
    /// RDA Status Data (metadata)
    /// </summary>
    RDA_STATUS_DATA = 2,
    /// <summary>
    /// Performance/Maintenance Data (metadata)
    /// </summary>
    PERFORMANCE_MAINTENANCE_DATA = 3,
    /// <summary>
    /// Console Message (RDA to RPG/RMS)
    /// </summary>
    CONSOLE_MESSAGE_RDA = 4,
    /// <summary>
    /// Volume Coverage Pattern (RDA to RPG, metadata)
    /// </summary>
    VOLUME_COVERAGE_PATTERN_RDA = 5,
    /// <summary>
    /// RDA Control Commands (RPG to RDA)
    /// </summary>
    RDA_CONTROL_COMMANDS = 6,
    /// <summary>
    /// Volume Coverage Pattern (RPG to RDA)
    /// </summary>
    VOLUME_COVERAGE_PATTERN_RPG = 7,
    /// <summary>
    /// Clutter Censor Zones (RPG to RDA)
    /// </summary>
    CLUTTER_CENSOR_ZONES = 8,
    /// <summary>
    /// Request for Data (RPG to RDA)
    /// </summary>
    REQUEST_FOR_DATA = 9,
    /// <summary>
    /// Console Message (RPG to RDA/RMS)
    /// </summary>
    CONSOLE_MESSAGE_RPG = 10,
    /// <summary>
    /// Loop Back Test (RDA to RPG)
    /// </summary>
    LOOP_BACK_TEST_RDA = 11,
    /// <summary>
    /// Loop Back Test (RPG to RDA)
    /// </summary>
    LOOP_BACK_TEST_RPG = 12,
    /// <summary>
    /// Clutter Filter Bypass Map (RDA to RPG, metadata)
    /// </summary>
    CLUTTER_FILTER_BYPASS_MAP = 13,
    /// <summary>
    /// Clutter Filter Map (RDA to RPG, metadata)
    /// </summary>
    CLUTTER_FILTER_MAP = 15,
    /// <summary>
    /// RDA Adaptation Data (RDA to RPG/RMS, metadata)
    /// </summary>
    RDA_ADAPTATION_DATA = 18,
    // NOTE: 20-26 are reserved or FAA/RMS only
    /// <summary>
    /// Digital Radar Data (Generic Format)
    /// </summary>
    GENERIC_FORMAT = 31,
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
    public const int SizeOf = 16;
    public MessageHeader(Span<byte> span) : this(
        BinaryPrimitives.ReadUInt16BigEndian(span[..2]),    //  Size
        span[2],                                            // Channels
        span[3],                                            // Type
        BinaryPrimitives.ReadUInt16BigEndian(span[4..6]),   // SequenceId
        BinaryPrimitives.ReadUInt16BigEndian(span[6..8]),   // Date
        BinaryPrimitives.ReadUInt32BigEndian(span[8..12]),  // Milliseconds
        BinaryPrimitives.ReadUInt16BigEndian(span[10..12]), // Segments
        BinaryPrimitives.ReadUInt16BigEndian(span[12..14])  // SegmentNumber
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
    public const int SizeOf = MessageHeader.SizeOf + 4 + 8;

    public MessageHeaderWithMetadata(Span<byte> span) : this(
        header: new MessageHeader(span[..MessageHeader.SizeOf]),
        recordNumber: BinaryPrimitives.ReadInt32BigEndian(span[MessageHeader.SizeOf..(MessageHeader.SizeOf + 4)]),
        filePosition: BinaryPrimitives.ReadInt64BigEndian(span[(MessageHeader.SizeOf + 4)..(MessageHeader.SizeOf + 12)])
    )
    { }
    public MessageHeaderWithMetadata(byte[] bytes) : this(bytes.AsSpan()) { }
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
    public const int SizeOf = 72;

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
    public Message31Header(byte[] bytes) : this(bytes.AsSpan()) { }

    /// <summary>
    /// Constructs a Message31Header from a Span<byte> (big-endian format).
    /// </summary>
    public Message31Header(Span<byte> span)
    {
        if (span.Length != SizeOf)
            throw new ArgumentException($"Span must be at least {SizeOf} bytes", nameof(span));

        // int offset = 0;

        // Read ID (4 bytes ASCII)
        _idBytes = new byte[4];
        span[0..4].CopyTo(_idBytes);

        CollectMilliseconds = BinaryPrimitives.ReadUInt32BigEndian(span[4..8]);
        CollectDate = BinaryPrimitives.ReadUInt16BigEndian(span[8..10]);
        AzimuthNumber = BinaryPrimitives.ReadUInt16BigEndian(span[10..12]);
        AzimuthAngle = BinaryPrimitives.ReadSingleBigEndian(span[12..16]);
        CompressFlag = span[16];
        Spare0 = span[17];
        RadialLength = BinaryPrimitives.ReadUInt16BigEndian(span[18..20]);
        // offset += 2;
        AzimuthResolution = span[20];
        RadialStatus = span[21];
        ElevationNumber = span[22];
        CutSector = span[23];
        ElevationAngle = BinaryPrimitives.ReadSingleBigEndian(span[24..28]);
        RadialBlanking = span[28];
        AzimuthMode = (sbyte)span[29];
        BlockCount = BinaryPrimitives.ReadUInt16BigEndian(span[30..32]);


        // Read block pointers (10 * 4 bytes = 40 bytes)
        _blockPointers = new uint[10];
        var start = 32;
        var stop = start + 4;
        for (int i = 0; i < 10; i++)
        {

            _blockPointers[i] = BinaryPrimitives.ReadUInt32BigEndian(span[start..stop]);
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
        LastRecordTimeUpdatePointer: BinaryPrimitives.ReadUInt16BigEndian(span[0..2]),
        VersionMajor: span[2],
        VersionMinor: span[3],
        Latitude: BinaryPrimitives.ReadSingleBigEndian(span[4..8]),
        Longitude: BinaryPrimitives.ReadSingleBigEndian(span[8..12]),
        Height: (short)BinaryPrimitives.ReadUInt16BigEndian(span[12..14]), // SINT2
        FeedhornHeight: BinaryPrimitives.ReadUInt16BigEndian(span[14..16]),
        ReflectivityCalibration: BinaryPrimitives.ReadSingleBigEndian(span[16..20]),
        PowerHorizontal: BinaryPrimitives.ReadSingleBigEndian(span[20..24]),
        PowerVertical: BinaryPrimitives.ReadSingleBigEndian(span[24..28]),
        DifferentialReflectivityCalibration: BinaryPrimitives.ReadSingleBigEndian(span[28..32]),
        InitialPhase: BinaryPrimitives.ReadSingleBigEndian(span[32..36]),
        VolumeCoveragePattern: BinaryPrimitives.ReadUInt16BigEndian(span[36..38])
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
        LastRecordTimeUpdatePointer: BinaryPrimitives.ReadUInt16BigEndian(span[0..2]),
        AtmosphericAttenuation: (short)BinaryPrimitives.ReadUInt16BigEndian(span[2..4]), // SINT2
        ReflectivityCalibration: BinaryPrimitives.ReadSingleBigEndian(span[4..8])
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
        LastRecordTimeUpdatePointer: BinaryPrimitives.ReadUInt16BigEndian(span[0..2]),
        UnambiguousRange: (short)BinaryPrimitives.ReadUInt16BigEndian(span[2..4]), // SINT2
        NoiseHorizontal: BinaryPrimitives.ReadSingleBigEndian(span[4..8]),
        NoiseVertical: BinaryPrimitives.ReadSingleBigEndian(span[8..12]),
        NyquistVelocity: (short)BinaryPrimitives.ReadUInt16BigEndian(span[12..14]) // SINT2
    )
    {
        if (span.Length < SizeOf)
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
public sealed record ConstantBlock(VolumeBlock? Volume = null, ElevationBlock? Elevation = null, RadialBlock? Radial = null) : DataBlock
{
    public ConstantBlock WithVolume(VolumeBlock volume) => this with { Volume = volume };
    public ConstantBlock WithElevation(ElevationBlock elevation) => this with { Elevation = elevation };
    public ConstantBlock WithRadial(RadialBlock radial) => this with { Radial = radial };
    public ConstantBlock With(DataName dataName, BinaryReader reader) => dataName switch
    {
        DataName.VOL => WithVolume(new VolumeBlock(reader.ReadBytes(VolumeBlock.SizeOf))),
        DataName.ELV => WithElevation(new ElevationBlock(reader.ReadBytes(ElevationBlock.SizeOf))),
        DataName.RAD => WithRadial(new RadialBlock(reader.ReadBytes(RadialBlock.SizeOf))),
        _ => throw new ArgumentException($"Unknown constant block type: {dataName}"),
    };

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
    MessageType MessageType,
    ConstantBlock ConstantBlock,
    Dictionary<DataName, DataBlock> Message31DataHeader,
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

