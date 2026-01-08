using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using SharpCompress.Compressors.BZip2;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
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
public class NexradLevel2Sweep(Dictionary<string, float[,]> data, double elevationAngle, double altitude, short rangeStart, short rangeScale, DateTime timestamp) : IDataAccessor<string, float[,]>, IEnumerable<KeyValuePair<string, float[,]>>
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
public class NexradLevel2Volume(Dictionary<int, Dictionary<string, float[,]>> data, IReadOnlyList<double> elevationAngles, Dictionary<int, (double altitude, short rangeStart, short rangeScale, DateTime timestamp)> sweepMetadata) : IDataAccessor<int, NexradLevel2Sweep>, IEnumerable<NexradLevel2Sweep>
{
    public readonly IReadOnlyList<double> ElevationAngles = elevationAngles;
    private readonly Dictionary<int, NexradLevel2Sweep> _sweeps = data.ToDictionary(
            kvp => kvp.Key,
            kvp =>
            {
                var metadata = sweepMetadata[kvp.Key];
                return new NexradLevel2Sweep(kvp.Value, elevationAngles[kvp.Key], metadata.altitude, metadata.rangeStart, metadata.rangeScale, metadata.timestamp);
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

/// <summary>
/// Provides key-based access to metadata headers.
/// </summary>
public class MetadataHeaderAccessor(Dictionary<string, List<MessageHeaderWithMetadata>> data) : IDataAccessor<string, IReadOnlyList<MessageHeaderWithMetadata>>
{
    private readonly Dictionary<string, List<MessageHeaderWithMetadata>> _data = data ?? throw new ArgumentNullException(nameof(data));

    public IEnumerable<string> Keys => _data.Keys;

    public IReadOnlyList<MessageHeaderWithMetadata> this[string key]
    {
        get
        {
            if (_data.TryGetValue(key, out var value))
                return value;
            throw new KeyNotFoundException($"Metadata key '{key}' not found. Available keys: {string.Join(", ", Keys)}");
        }
    }

    public bool TryGetValue(string key, out IReadOnlyList<MessageHeaderWithMetadata> value)
    {
        if (_data.TryGetValue(key, out var list))
        {
            value = list;
            return true;
        }
        value = [];
        return false;
    }

    public bool ContainsKey(string key) => _data.ContainsKey(key);

    /// <summary>
    /// Gets the number of metadata message types.
    /// </summary>
    public int Count => _data.Count;
    public IEnumerator<IReadOnlyList<MessageHeaderWithMetadata>> GetEnumerator() => _data.Values.GetEnumerator();

    // IEnumerator<KeyValuePair<string, IReadOnlyList<MessageHeaderWithMetadata>>> IEnumerable<KeyValuePair<string, IReadOnlyList<MessageHeaderWithMetadata>>>.GetEnumerator() => _data.GetEnumerator();

    // IEnumerator IEnumerable.GetEnumerator() => _data.GetEnumerator();
}


// ================================================================================================================= //
#region constructor methods
// ================================================================================================================= //
public class NexradLevel2Reader(Stream input, bool leaveOpen = false) : BinaryReader(input, Encoding.ASCII, leaveOpen)
{
    public static NexradLevel2Reader Open(string fileName, bool leaveOpen = false)
    {
        if (!File.Exists(fileName))
        {
            throw new FileNotFoundException(fileName);
        }
        var f = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
        return new NexradLevel2Reader(f, leaveOpen);
    }
    #endregion
    // ============================================================================================================= //
    #region public methods
    // ============================================================================================================= //
    /// <summary>
    /// Opens sweeps from the NEXRAD file and returns them as a NexradLevel2Volume for easy key-based access.
    /// </summary>
    /// <param name="sweeps">Optional list of sweep numbers to extract. If null, extracts all sweeps.</param>
    /// <returns>A NexradLevel2Volume that provides key-based access to sweep data</returns>
    /// <example>
    /// <code>
    /// using var reader = NexradLevel2Reader.Open("file.nexrad");
    /// var sweeps = reader[..];
    /// var sweep0 = sweeps[0];
    /// var reflectivity = sweep0["DBZH"]; // float[,] array
    /// </code>
    /// </example>
    /// <summary>
    /// Extract metadata (elevations, quantities, coordinates) from file headers without reading sweep data.
    /// This is much faster than loading the full file.
    /// </summary>
    public static List<ScanMetadata> ExtractMetadata(Stream stream)
    {
        using var reader = new NexradLevel2Reader(stream);
        return ExtractMetadata(reader);
    }
    public string GetInstrumentName()
    {
        return ExtractInstrumentName();
    }
    public NexradLevel2Sweep this[int sweepNumber]
    {
        get
        {
            var (_, _, msg31DataHeader) = DataHeaders;
            var sweep = msg31DataHeader[sweepNumber];
            var data = ReadSweepData(sweep);
            var elevationAngles = ElevationAngles;
            var (altitude, rangeStart, rangeScale, timestamp) = ExtractSweepMetadata(sweep);
            return new NexradLevel2Sweep(data, elevationAngles[sweepNumber], altitude, rangeStart, rangeScale, timestamp);
        }
    }
    public NexradLevel2Volume this[IEnumerable<int> sweeps]
    {
        get
        {
            var (_, _, msg31DataHeader) = DataHeaders;
            var sweepNumbers = sweeps?.ToList() ?? Enumerable.Range(0, msg31DataHeader.Count).ToList();
            var data = OpenSweepData(sweepNumbers);
            var metadata = sweepNumbers.ToDictionary(
                sweepNum => sweepNum,
                sweepNum => ExtractSweepMetadata(msg31DataHeader[sweepNum])
            );
            return new NexradLevel2Volume(data, ElevationAngles, metadata);
        }
    }
    public NexradLevel2Volume this[Range range]
    {
        get
        {
            var (_, _, msg31DataHeader) = DataHeaders;
            Dictionary<int, Dictionary<string, float[,]>>? data;
            List<int> sweepNumbers;
            if (range.Equals(..))
            {
                sweepNumbers = Enumerable.Range(0, msg31DataHeader.Count).ToList();
                data = OpenSweepData(null);
            }
            else
            {
                sweepNumbers = [.. Enumerable.Range(range.Start.Value, range.End.Value)];
                data = OpenSweepData(sweepNumbers);
            }

            var elevationAngles = ElevationAngles;
            var metadata = sweepNumbers.ToDictionary(
                sweepNum => sweepNum,
                sweepNum => ExtractSweepMetadata(msg31DataHeader[sweepNum])
            );
            return new NexradLevel2Volume(data, elevationAngles, metadata);
        }
    }

    /// <summary>
    /// Gets a single sweep's data as a NexradLevel2Sweep for easy key-based access.
    /// </summary>
    /// <param name="sweepNumber">The zero-based sweep number</param>
    /// <returns>A NexradLevel2Sweep that provides key-based access to the sweep's variables</returns>
    /// <example>
    /// <code>
    /// using var reader = NexradLevel2Reader.Open("file.nexrad");
    /// var sweep = reader.GetSweepAccessor(0);
    /// var reflectivity = sweep["DBZH"]; // float[,] array
    /// var velocity = sweep["VRADH"]; // float[,] array
    /// </code>
    /// </example>
    public NexradLevel2Sweep GetSweepAccessor(int sweepNumber)
    {
        var (_, _, msg31DataHeader) = DataHeaders;
        if (sweepNumber < 0 || sweepNumber >= msg31DataHeader.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(sweepNumber),
                $"Sweep number {sweepNumber} is out of range. Available sweeps: 0-{msg31DataHeader.Count - 1}");
        }
        var sweep = msg31DataHeader[sweepNumber];
        var data = ReadSweepData(sweep);
        var elevationAngles = ElevationAngles;
        var (altitude, rangeStart, rangeScale, timestamp) = ExtractSweepMetadata(sweep);
        return new NexradLevel2Sweep(data, elevationAngles[sweepNumber], altitude, rangeStart, rangeScale, timestamp);
    }

    /// <summary>
    /// Gets metadata headers as a MetadataHeaderAccessor for easy key-based access.
    /// </summary>
    /// <returns>A MetadataHeaderAccessor that provides key-based access to metadata headers</returns>
    /// <example>
    /// <code>
    /// using var reader = NexradLevel2Reader.Open("file.nexrad");
    /// var metadata = reader.GetMetadataHeaderAccessor();
    /// var msg2Headers = metadata["msg_2"]; // List of MessageHeaderWithMetadata
    /// </code>
    /// </example>
    public MetadataHeaderAccessor GetMetadataHeaderAccessor()
    {
        var data = GetMetadataHeader();
        return new MetadataHeaderAccessor(data);
    }

    /// <summary>
    /// Gets a list of all elevation angles for each sweep in degrees.
    /// </summary>
    /// <returns>A list of elevation angles, one per sweep, in degrees</returns>
    /// <example>
    /// <code>
    /// using var reader = NexradLevel2Reader.Open("file.nexrad");
    /// var elevations = reader.ElevationAngles;
    /// // elevations[0] is the elevation angle for sweep 0
    /// </code>
    /// </example>
    public IReadOnlyList<double> ElevationAngles
    {
        get
        {
            var (_, msg31Header, _) = DataHeaders;
            var elevations = new List<double>(msg31Header.Count);

            foreach (var sweepHeaders in msg31Header)
            {
                if (sweepHeaders.Count > 0)
                {
                    var firstHeader = sweepHeaders[0];
                    if (firstHeader is Message31Header msg31)
                    {
                        elevations.Add(msg31.ElevationAngle);
                    }
                    else if (firstHeader is Message1Header msg1)
                    {
                        // Message1Header stores elevation as ushort (CODE2 format)
                        // Convert to degrees: elevation_angle / 8.0
                        elevations.Add(msg1.ElevationAngle / 8.0);
                    }
                    else
                    {
                        elevations.Add(double.NaN);
                    }
                }
                else
                {
                    elevations.Add(double.NaN);
                }
            }

            return elevations;
        }
    }


    private IEnumerable<(string, IConvertible)> RepresentationFields => [


    ];
    public override string ToString()
    {
        return string.Join(", ", RepresentationFields.Select((k, v) => $"{k}: {v}"));
    }
    #endregion
    // ============================================================================================================= //
    #region private methods
    // ============================================================================================================= //

    // Helper methods for reading big-endian values
    private ushort ReadUInt16BE()
    {
        var bytes = ReadBytes(2);
        return (ushort)((bytes[0] << 8) | bytes[1]);
    }


    private static readonly Dictionary<string, string> _nexradMapping = new()
    {
        ["REF"] = "DBZH",
        ["VEL"] = "VRADH",
        ["SW "] = "WRADH",
        ["ZDR"] = "ZDR",
        ["PHI"] = "PHIDP",
        ["RHO"] = "RHOHV",
        ["CFP"] = "CCORH",
    };
    // NEXRAD Level II file structures and sizes
    // The details on these structures are documented in:
    // "Interface Control Document for the Archive II/User" RPG Build 12.0
    // Document Number 2620010E and
    // "Interface Control Document for the RDA/RPG" Open Build 13.0
    // Document Number 2620002M
    // NEXRAD Level II constants
    private const int RECORD_BYTES = 2432;
    private const int COMPRESSION_RECORD_SIZE = 12;
    private const int CONTROL_WORD_SIZE = 4;

    // # Table II Message Header Data
    // Message header sizes are now calculated using struct SizeOf properties
    // MessageHeader.SizeOf = 16 bytes
    // Message31Header has variable size due to BlockPointers array, calculated as: 4 + 4 + 2 + 2 + 4 + 1 + 1 + 2 + 1 + 1 + 1 + 1 + 4 + 1 + 1 + 2 + (10 * 4) = 72 bytes
    // Message1Header.SizeOf = 100 bytes

    // Helper methods for reading big-endian values
    private static ushort ReadUInt16BE(BinaryReader br)
    {
        var bytes = br.ReadBytes(2);
        return (ushort)((bytes[0] << 8) | bytes[1]);
    }

    private static uint ReadUInt32BE(BinaryReader br)
    {
        var bytes = br.ReadBytes(4);
        return (uint)((bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3]);
    }

    private static float ReadFloat32BE(BinaryReader br)
    {
        var bytes = br.ReadBytes(4);
        // Big-endian bytes from stream: [MSB, ..., LSB] = [b0, b1, b2, b3]
        // BitConverter.ToSingle on little-endian systems expects [LSB, ..., MSB] = [b3, b2, b1, b0]
        // So we need to reverse the bytes when on a little-endian system
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes);
        }
        return BitConverter.ToSingle(bytes, 0);
    }

    private long _recordStart = 0; // Start position of current record in file
    private long _recordPos = 0;   // Current position within the current record
    private int _recordNumber = 0; // Current record number
    private int _recordSize = RECORD_BYTES; // Size of current record

    // For compressed files: cache decompressed LDM blocks
    private readonly Dictionary<int, byte[]> _localDataManager = []; // LDM number -> decompressed data
    private List<long>? _bz2RecordIndices = null; // File offsets of bzip2 records

    private bool _isCompressed = false;
    private bool _compressionChecked = false;

    public bool IsCompressed()
    {
        if (_compressionChecked) return _isCompressed;
        // Check if file is compressed by reading bytes 24-28
        // Python: size = self._fh[24:28].view(dtype=">u4")[0]
        var savedPos = BaseStream.Position;
        BaseStream.Position = 24;
        var size = ReadUInt32BE(this);
        BaseStream.Position = savedPos;
        _isCompressed = size > 0;
        _compressionChecked = true;

        return _isCompressed;
    }

    /// <summary>
    /// Get file offsets of bzip2 records.
    /// Python equivalent: bz2_record_indices property
    /// </summary>
    private List<long> GetBz2RecordIndices()
    {
        if (_bz2RecordIndices != null) return _bz2RecordIndices;

        _bz2RecordIndices = [];

        // Read entire file into memory to search for bzip2 magic
        var savedPos = BaseStream.Position;
        BaseStream.Position = 0;
        List<byte> fileBytes = [];
        var buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = Read(buffer, 0, buffer.Length)) > 0)
        {
            fileBytes.AddRange(buffer.Take(bytesRead));
        }
        BaseStream.Position = savedPos;

        // Search for bzip2 magic: BZhX1AY&SY (where X is 0-9)
        // Python: seq = np.array([66, 90, 104, 0, 49, 65, 89, 38, 83, 89], dtype=np.uint8)
        // Python: rd = util.rolling_dim(self._fh, len(seq))
        // Python: self._bz2_indices = np.nonzero((rd == seq).sum(1) >= 9)[0] - 4
        // This means: find positions where at least 9 out of 10 bytes match the pattern
        // Pattern: [66, 90, 104, X, 49, 65, 89, 38, 83, 89] where X can be any byte
        var pattern = new byte[] { 66, 90, 104, 0, 49, 65, 89, 38, 83, 89 }; // X is wildcard (position 3)

        for (int i = 0; i < fileBytes.Count - pattern.Length; i++)
        {
            int matches = 0;
            for (int j = 0; j < pattern.Length; j++)
            {
                // Position 3 is wildcard (X can be 0-9, but we'll accept any byte)
                if (j == 3) matches++;
                else if (i + j < fileBytes.Count && fileBytes[i + j] == pattern[j]) matches++;
            }
            // If at least 9 out of 10 bytes match, we found a bzip2 record
            if (matches >= 9)
            {
                // Found bzip2 record, offset is 4 bytes before BZh (where size is stored)
                _bz2RecordIndices.Add(i - 4);
            }
        }

        return _bz2RecordIndices;
    }

    /// <summary>
    /// Map record number to LDM number.
    /// Python equivalent: get_ldm(recordNumber)
    /// </summary>
    private static int GetLdm(int recordNumber) => recordNumber < 134 ? 0 : ((recordNumber - 134) / 120) + 1;


    /// <summary>
    /// Decompress an LDM block.
    /// </summary>
    private byte[]? DecompressLdm(int ldmKey)
    {
        if (_localDataManager.TryGetValue(ldmKey, out byte[]? arr)) return arr;


        var bz2Indices = GetBz2RecordIndices();
        if (ldmKey >= bz2Indices.Count) return null;


        var start = bz2Indices[ldmKey];
        BaseStream.Position = start;

        // Read bzip2 size (4 bytes, big-endian)
        var bz2Size = ReadUInt32BE(this);
        if (bz2Size == 0 || bz2Size > int.MaxValue) return null;

        // Read compressed data
        var compressed = ReadBytes((int)bz2Size);


        // Decompress
        arr = DecompressBz2(compressed);


        _localDataManager[ldmKey] = arr;
        return arr;
    }
    private static byte[] DecompressBz2(byte[] compressed)
    {
        using var ms = new MemoryStream(compressed);
        using var bz2 = new BZip2Stream(ms, SharpCompress.Compressors.CompressionMode.Decompress, decompressConcatenated: false);
        using var s = new MemoryStream();
        bz2.CopyTo(s);
        return s.ToArray();
    }

    /// <summary>
    /// Get record size from message header.
    /// Python equivalent: get_end(buf)
    /// </summary>
    private static int GetRecordSize(byte[] recordData, int offset)
    {
        if (offset + 16 > recordData.Length) return 0;


        // Read message header at offset + 12 (skip 12-byte record header)
        var msgHeaderOffset = offset + 12;
        if (msgHeaderOffset + 16 > recordData.Length) return 0;


        // Read size field (first 2 bytes of message header, big-endian)
        var sizeBytes = new byte[2];
        Array.Copy(recordData, msgHeaderOffset, sizeBytes, 0, 2);
        var size = (ushort)((sizeBytes[0] << 8) | sizeBytes[1]);

        // Read type (4th byte of message header)
        var msgType = recordData[msgHeaderOffset + 3];

        // Calculate record size
        var recordSize = size * 2 + 12; // size is in halfwords, + 12 for record header
        if (msgType != 31)
        {
            // For non-message-31, minimum size is RECORD_BYTES
            if (recordSize < RECORD_BYTES)
            {
                recordSize = RECORD_BYTES;
            }
        }

        return recordSize;
    }

    /// <summary>
    /// Get next record from file.
    /// Python equivalent: init_next_record()
    /// </summary>
    public bool InitNextRecord() => InitRecord(_recordNumber + 1);

    /// <summary>
    /// Initialize record using given number.
    /// Python equivalent: init_record(recordNumber)
    /// </summary>
    public bool InitRecord(int recordNumber)
    {
        if (IsCompressed())
        {
            var ldmKey = GetLdm(recordNumber);

            // Decompress LDM block if needed
            var ldmData = DecompressLdm(ldmKey);
            if (ldmData == null) return false;

            if (recordNumber < 134)
            {
                // Records 0-133: fixed size, starting at recordNumber * RECORD_BYTES
                _recordStart = recordNumber * RECORD_BYTES;
                _recordSize = RECORD_BYTES;
            }
            else
            {
                // Records >= 134: variable size, need to find start position
                // Get index into current LDM: rnum = (recordNumber - 134) % 120
                var rnum = (recordNumber - 134) % 120;

                if (rnum == 0)
                {
                    // First record in this LDM starts at position 0
                    _recordStart = 0;
                }
                else
                {
                    // Python: start = self.record_size + self.filepos if rnum else 0
                    // For subsequent records, we need to find the start by reading from the previous record
                    // We'll track position by reading through records from the start of the LDM
                    _recordStart = 0;
                    for (int i = 0; i < rnum; i++)
                    {
                        var recordSize = GetRecordSize(ldmData, (int)_recordStart);
                        if (recordSize == 0) return false;
                        _recordStart += recordSize;
                        if (_recordStart >= ldmData.Length) return false;
                    }
                }

                // Get size of this record by reading message header
                // Python: buf = self._ldm[ldmKey][start + 12 : start + 12 + LEN_MSG_HEADER]
                // Python: size = self.get_end(buf)
                var msgHeaderOffset = (int)_recordStart + 12;
                if (msgHeaderOffset + 16 > ldmData.Length) return false;

                // Read message header to get size
                var sizeBytes = new byte[2];
                Array.Copy(ldmData, msgHeaderOffset, sizeBytes, 0, 2);
                var size = (ushort)((sizeBytes[0] << 8) | sizeBytes[1]);
                var msgType = ldmData[msgHeaderOffset + 3];

                _recordSize = size * 2 + 12; // size is in halfwords, + 12 for record header
                if (msgType != 31 && _recordSize < RECORD_BYTES)
                {
                    _recordSize = RECORD_BYTES;
                }

                if (_recordStart + _recordSize > ldmData.Length) return false;
            }

            _recordPos = 0;
            _recordNumber = recordNumber;
            return true;
        }
        else
        {
            // Uncompressed files
            if (recordNumber < 134)
            {
                // Fixed size records
                _recordStart = recordNumber * RECORD_BYTES + 24; // +24 for volume header
                _recordSize = RECORD_BYTES;
            }
            else
            {
                // Variable size records - need to find start
                // For now, use a simple approach: start from last known position
                // This is simplified - proper implementation would track positions
                var start = _recordStart + _recordSize;
                BaseStream.Position = start;

                // Read message header to get size
                BaseStream.Position += 12; // Skip record header
                var size = ReadUInt16BE();
                var msgType = ReadByte();
                BaseStream.Position = start;

                var recordSize = size * 2 + 12;
                if (msgType != 31 && recordSize < RECORD_BYTES)
                {
                    recordSize = RECORD_BYTES;
                }

                _recordStart = start;
                _recordSize = recordSize;
            }

            BaseStream.Position = _recordStart;
            _recordPos = 0;
            _recordNumber = recordNumber;
            return true;
        }
    }

    /// <summary>
    /// Read and unpack message header.
    /// Python equivalent: get_message_header()
    /// Note: Must call InitRecord() first to position at the start of a record
    /// </summary>
    public MessageHeader GetMessageHeader()
    {
        BinaryReader? tempReader = null;
        BinaryReader readerToUse = this;
        bool createdTempReader = false;

        // For compressed files, use decompressed data
        if (IsCompressed())
        {
            var ldmKey = GetLdm(_recordNumber);
            if (_localDataManager.TryGetValue(ldmKey, out byte[]? ldmData))
            {
                var ms = new MemoryStream(ldmData)
                {
                    Position = _recordStart
                };
                tempReader = new BinaryReader(ms);
                readerToUse = tempReader;
                createdTempReader = true;
            }
            else
            {
                BaseStream.Position = _recordStart;
            }
        }
        else
        {
            BaseStream.Position = _recordStart;
        }

        // Skip 12-byte record header (Python: self._rh.pos += 12)
        _recordPos = 12;
        if (createdTempReader)
        {
            readerToUse.BaseStream.Position = _recordStart + _recordPos;
        }
        else
        {
            BaseStream.Position = _recordStart + _recordPos;
        }

        // Read MSG_HEADER structure (16 bytes, big-endian)
        var header = MessageHeader.Read(readerToUse);
        _recordPos += MessageHeader.SizeOf; // Update position after reading header



        // Clean up temporary reader if we created one
        if (createdTempReader && tempReader != null)
        {
            tempReader.Dispose();
        }

        return header;
    }

    /// <summary>
    /// Get metadata header.
    /// Python equivalent: get_metadata_header()
    /// </summary>
    public Dictionary<string, List<MessageHeaderWithMetadata>> GetMetadataHeader()
    {
        // """Get metadaata header"""
        // # data offsets
        // # ICD 2620010J
        // # 7.3.5 Metadata Record
        // # the above document will evolve over time
        // # please revisit and adopt accordingly
        var meta_headers = new Dictionary<string, List<MessageHeaderWithMetadata>>();
        var rec = 0;

        // iterate over all messages until type outside [2, 3, 5, 13, 15, 18, 32]
        while (true)
        {
            if (!InitRecord(rec)) break;

            // Get filepos before reading message header (Python: filepos = self.filepos)
            // For compressed files, filepos is the position in the decompressed LDM
            // For uncompressed files, filepos is the position in the original file
            long filepos = _recordStart;

            var message_header = GetMessageHeader();

            // do not read zero blocks of data
            if (message_header.Type == 0)
            {
                rec += 1;
                continue;
            }

            // stop if first non meta header is found
            if (message_header.Type is not (2 or 3 or 5 or 13 or 15 or 18 or 32)) break;

            // Create header with metadata
            var headerWithMeta = new MessageHeaderWithMetadata(
                message_header,     // Header
                rec,                // RecordNumber
                filepos             // FilePosition
            );

            // Add to appropriate msg_{type} list
            var msgTypeKey = $"msg_{message_header.Type}";
            if (!meta_headers.TryGetValue(msgTypeKey, out List<MessageHeaderWithMetadata>? value))
            {
                value = [];
                meta_headers[msgTypeKey] = value;
            }

            value.Add(headerWithMeta);

            rec += 1;
        }

        return meta_headers;
    }

    private Dictionary<string, List<MessageHeaderWithMetadata>>? _metaHeader = null;

    /// <summary>
    /// Get metadata header (cached).
    /// Python equivalent: self.meta_header property
    /// </summary>
    public Dictionary<string, List<MessageHeaderWithMetadata>> MetaHeader
    {
        get
        {
            _metaHeader ??= GetMetadataHeader();
            return _metaHeader;
        }
    }
    private (List<MessageHeader> dataHeader, List<List<IMessageHeader>> msg31Header, List<SweepData> msg31DataHeader)? _dataHeaders = null;
    public (List<MessageHeader> dataHeader, List<List<IMessageHeader>> msg31Header, List<SweepData> msg31DataHeader) DataHeaders
    {
        get
        {
            _dataHeaders ??= _GetDataHeader();
            return _dataHeaders.Value;
        }
    }

    /// <summary>
    /// Load all data header from file.
    /// Python equivalent: get_data_header()
    /// Returns: (data_header, _msg_31_header, _msg_31_data_header)
    /// </summary>
    public (List<MessageHeader> dataHeader, List<List<IMessageHeader>> msg31Header, List<SweepData> msg31DataHeader) _GetDataHeader()
    {
        // get the record number from the meta header
        // message 2 is the last meta header, after which the data records are located
        var metaHeader = MetaHeader;
        if (!metaHeader.TryGetValue("msg_2", out List<MessageHeaderWithMetadata>? value) || value.Count == 0)
        {
            throw new InvalidOperationException("No msg_2 found in metadata header");
        }

        var recordNumber = value[0].RecordNumber;
        if (!InitRecord(recordNumber))
        {
            throw new InvalidOperationException($"Failed to initialize record {recordNumber}");
        }

        int currentSweep = -1;
        int currentHeader = -1;
        var sweepMsg31Header = new List<IMessageHeader>();
        var sweepIntermediateRecords = new List<int>();

        var dataHeader = new List<MessageHeader>();
        var msg31Header = new List<List<IMessageHeader>>();
        var msg31DataHeader = new List<SweepData>();

        int maxRecords = 10000; // Safety limit
        int recordCount = 0;

        // Process records starting from the one after msg_2
        while (recordCount < maxRecords && InitNextRecord())
        {
            recordCount++;
            currentHeader += 1;
            // get message headers
            var msgHeader = GetMessageHeader();

            // keep all data headers
            dataHeader.Add(msgHeader);

            // check which message type we have (1 or 31)
            if (msgHeader.Type == 1 || msgHeader.Type == 31)
            {
                // Read the full message header (MSG_1 or MSG_31)
                var msg31HeaderObj = (IMessageHeader)ReadMessage31Or1Header(msgHeader.Type);

                // retrieve data/const headers from msg 31
                // check if this is a new sweep
                SweepStatus status = msg31HeaderObj.GetStatus();



                if (status == SweepStatus.IntermediateRadial) {/** 1 - intermediate radial pass */}
                else if (status == SweepStatus.EndOfElevation || status == SweepStatus.EndOfVolume)
                {
                    // 2 - end of elevation
                    // 4 - end of volume
                    if (msg31DataHeader.Count > 0)
                    {
                        var lastSweep = msg31DataHeader[^1];
                        msg31DataHeader[^1] = lastSweep with
                        {
                            RecordEnd = _recordNumber,
                            IntermediateRecords = sweepIntermediateRecords.Count > 0 ? sweepIntermediateRecords : null
                        };
                    }
                    // Note: In Python this is self._data[current_sweep] = sweep
                    // but we're not storing it in a class field, just in the return value
                    msg31Header.Add(sweepMsg31Header);
                }
                else if (status == SweepStatus.StartOfNewElevation || status == SweepStatus.StartOfNewVolume || status == SweepStatus.StartOfNewElevationLastElevationInVCP)
                {
                    // 0 - start of new elevation
                    // 3 - start of new volume
                    // 5 - start of new elevation, last elevation in VCP
                    currentSweep += 1;
                    // create new sweep object
                    sweepMsg31Header = [];
                    sweepIntermediateRecords = [];

                    // new message 31 data
                    if (msgHeader.Type == 31 && msg31HeaderObj is Message31Header msg31Hdr)
                        msg31DataHeader.Add(new SweepData(
                            RecordNumber: _recordNumber,
                            FilePosition: _recordStart,
                            Message31Header: msg31Hdr,
                            MessageType: msgHeader.Type,
                            Message31DataHeader: ParseMessage31DataBlocks(msg31Hdr)
                        ));

                    // former message 1 data - convert to Message31Header
                    else if (msgHeader.Type == 1 && msg31HeaderObj is Message1Header msg1Hdr)
                    {
                        // Convert Message1Header to Message31Header
                        var msg31FromMsg1 = new Message31Header(
                            id: "MSG1",
                            collectMilliseconds: msg1Hdr.CollectMilliseconds,
                            collectDate: msg1Hdr.CollectDate,
                            azimuthNumber: msg1Hdr.AzimuthNumber,
                            azimuthAngle: msg1Hdr.AzimuthAngle,
                            compressFlag: 0,
                            spare0: 0,
                            radialLength: 0,
                            azimuthResolution: 0,
                            radialStatus: (byte)msg1Hdr.RadialStatus,
                            elevationNumber: (byte)msg1Hdr.ElevationNumber,
                            cutSector: (byte)msg1Hdr.CutSectorNumber,
                            elevationAngle: msg1Hdr.ElevationAngle,
                            radialBlanking: 0,
                            azimuthMode: 0,
                            blockCount: 0,
                            blockPointers: Array.Empty<uint>()
                        );
                        msg31DataHeader.Add(new SweepData(
                            RecordNumber: _recordNumber,
                            FilePosition: _recordStart,
                            Message31Header: msg31FromMsg1,
                            MessageType: msgHeader.Type,
                            Message31DataHeader: ParseMessage1DataBlocks(msg1Hdr)
                        ));
                    }


                }

                sweepMsg31Header.Add(msg31HeaderObj);
            }
            else
            {
                sweepIntermediateRecords.Add(_recordNumber);
            }
        }

        // Ensure the last sweep is finalized if not already closed by status 2/4
        if (msg31DataHeader.Count > 0)
        {
            var lastSweep = msg31DataHeader[^1];
            if (lastSweep.RecordEnd == null)
            {
                msg31DataHeader[^1] = lastSweep with
                {
                    RecordEnd = _recordNumber,
                    IntermediateRecords = sweepIntermediateRecords.Count > 0 ? sweepIntermediateRecords : null
                };
            }
            msg31Header.Add(sweepMsg31Header);
        }

        return (dataHeader, msg31Header, msg31DataHeader);
    }

    /// <summary>
    /// Read MSG_31 or MSG_1 header after the message header.
    /// Python equivalent: _unpack_dictionary(self._rh.read(msg_len, width=1), msg, ...)
    /// </summary>
    private IMessageHeader ReadMessage31Or1Header(byte msgType)
    {
        BinaryReader? tempReader = null;
        BinaryReader readerToUse = this;
        bool createdTempReader = false;

        // For compressed files, use decompressed data
        if (IsCompressed())
        {
            var ldmKey = GetLdm(_recordNumber);
            if (_localDataManager.TryGetValue(ldmKey, out byte[]? ldmData))
            {
                var ms = new MemoryStream(ldmData)
                {
                    Position = _recordStart + _recordPos
                };
                tempReader = new BinaryReader(ms);
                readerToUse = tempReader;
                createdTempReader = true;
            }
            else
            {
                BaseStream.Position = _recordStart + _recordPos;
            }
        }
        else
        {
            BaseStream.Position = _recordStart + _recordPos;
        }

        IMessageHeader header;

        if (msgType == 31)
        {
            header = Message31Header.Read(readerToUse);
            _recordPos += Message31Header.SizeOf;
        }
        else
        {
            header = Message1Header.Read(readerToUse);
            _recordPos += Message1Header.SizeOf;
        }

        // Clean up temporary reader if we created one
        if (createdTempReader && tempReader != null)
        {
            tempReader.Dispose();
        }

        return header;
    }

    /// <summary>
    /// Parse data blocks from Message 31.
    /// Python equivalent: parsing block_pointers and DATA_BLOCK_HEADER
    /// </summary>
    private Dictionary<string, DataBlock> ParseMessage31DataBlocks(Message31Header msg31Header)
    {
        var blockHeader = new Dictionary<string, DataBlock>();

        var blockPointers = msg31Header.BlockPointers.Where(bp => bp > 0).ToList();

        var countToProcess = Math.Min(blockPointers.Count, msg31Header.BlockCount);

        BinaryReader? tempReader = null;
        BinaryReader readerToUse = this;
        bool createdTempReader = false;

        // For compressed files, use decompressed data
        if (IsCompressed())
        {
            var ldmKey = GetLdm(_recordNumber);
            if (_localDataManager.TryGetValue(ldmKey, out byte[]? ldmData))
            {
                var ms = new MemoryStream(ldmData);
                tempReader = new BinaryReader(ms);
                readerToUse = tempReader;
                createdTempReader = true;
            }
        }

        for (int i = 0; i < countToProcess; i++)
        {
            var blockPointer = blockPointers[i];
            // Python: self.rh.pos = block_pointer + 12 + MessageHeader.SizeOf
            var targetPos = (long)blockPointer + 12 + MessageHeader.SizeOf;

            if (createdTempReader)
            {
                readerToUse.BaseStream.Position = _recordStart + targetPos;
            }
            else
            {
                BaseStream.Position = _recordStart + targetPos;
            }

            // Read DATA_BLOCK_HEADER (4 bytes: block_type (1 byte) + data_name (3 bytes))
            var blockTypeBytes = readerToUse.ReadBytes(1);
            var dataNameBytes = readerToUse.ReadBytes(3);
            var blockType = Encoding.ASCII.GetString(blockTypeBytes);
            var dataName = Encoding.ASCII.GetString(dataNameBytes).TrimEnd('\0');

            // Parse the block based on type
            if (blockType == "R")
            {
                // Constant block (VOL, ELV, RAD)
                var blockData = ParseConstantBlock(readerToUse, dataName);
                blockHeader[dataName] = blockData;
            }
            else if (blockType == "D")
            {
                // Variable block (REF, VEL, SW, ZDR, PHI, RHO, CFP)
                // Store data_offset relative to the start of the current record
                var variableBlock = VariableBlock.Read(readerToUse, readerToUse.BaseStream.Position - _recordStart);
                blockHeader[dataName] = new VariableDataBlock(variableBlock);
            }
        }

        if (createdTempReader && tempReader != null)
        {
            tempReader.Dispose();
        }

        return blockHeader;
    }

    /// <summary>
    /// Parse constant data block (VOL, ELV, RAD).
    /// </summary>
    private ConstantDataBlock ParseConstantBlock(BinaryReader br, string dataName)
    {
        if (dataName == "VOL")
        {
            var result = new VolumeBlock(
                LastRecordTimeUpdatePointer: ReadUInt16BE(br),
                VersionMajor: br.ReadByte(),
                VersionMinor: br.ReadByte(),
                Latitude: ReadFloat32BE(br),
                Longitude: ReadFloat32BE(br),
                Height: (short)ReadUInt16BE(br), // SINT2
                FeedhornHeight: ReadUInt16BE(br),
                ReflectivityCalibration: ReadFloat32BE(br),
                PowerHorizontal: ReadFloat32BE(br),
                PowerVertical: ReadFloat32BE(br),
                DifferentialReflectivityCalibration: ReadFloat32BE(br),
                InitialPhase: ReadFloat32BE(br),
                VolumeCoveragePattern: ReadUInt16BE(br)
            );
            br.ReadBytes(2); // spare (2 bytes)
            return ConstantDataBlock.FromVolume(result);
        }
        else if (dataName == "ELV")
        {
            var result = new ElevationBlock(
                LastRecordTimeUpdatePointer: ReadUInt16BE(br),
                AtmosphericAttenuation: (short)ReadUInt16BE(br), // SINT2
                ReflectivityCalibration: ReadFloat32BE(br)
            );
            return ConstantDataBlock.FromElevation(result);
        }
        else if (dataName == "RAD")
        {
            var result = new RadialBlock(
                LastRecordTimeUpdatePointer: ReadUInt16BE(br),
                UnambiguousRange: (short)ReadUInt16BE(br), // SINT2
                NoiseHorizontal: ReadFloat32BE(br),
                NoiseVertical: ReadFloat32BE(br),
                NyquistVelocity: (short)ReadUInt16BE(br) // SINT2
            );
            br.ReadBytes(2); // spare (2 bytes)
            return ConstantDataBlock.FromRadial(result);
        }
        throw new ArgumentException($"Unknown constant block type: {dataName}");
    }



    /// <summary>
    /// Parse data blocks from Message 1.
    /// Python equivalent: creating block_pointers from MSG_1 structure
    /// </summary>
    private Dictionary<string, DataBlock> ParseMessage1DataBlocks(Message1Header msg1Header)
    {
        var blockHeader = new Dictionary<string, DataBlock>
        {
            // Create VOL block
            ["VOL"] = ConstantDataBlock.FromVolume(new VolumeBlock(
                LastRecordTimeUpdatePointer: 0,
                VersionMajor: 0,
                VersionMinor: 0,
                Latitude: 0,
                Longitude: 0,
                Height: 0,
                FeedhornHeight: 0,
                ReflectivityCalibration: 0,
                PowerHorizontal: 0,
                PowerVertical: 0,
                DifferentialReflectivityCalibration: 0,
                InitialPhase: 0,
                VolumeCoveragePattern: msg1Header.VolumeCoveragePattern
            ))
        };

        // Create block pointers for REF, VEL, SW
        var velScale = msg1Header.DopplerResolution switch
        {
            2 => 2.0f,
            4 => 1.0f,
            _ => 0.0f
        };

        // Only add blocks with ngates > 0
        if (msg1Header.SurveillanceNumberOfBins > 0)
        {
            blockHeader["REF"] = new VariableDataBlock(new VariableBlock(
                Reserved: 0,
                NumberOfGates: msg1Header.SurveillanceNumberOfBins,
                FirstGate: (short)msg1Header.SurveillanceRangeFirst,
                GateSpacing: (short)msg1Header.SurveillanceRangeStep,
                Threshold: 0,
                SignalToNoiseRatioThreshold: 0,
                Flags: 0,
                WordSize: 8,
                Scale: 2.0f,
                Offset: 66.0f,
                DataOffset: msg1Header.SurveillancePointer + MessageHeader.SizeOf + 12
            ));
        }
        if (msg1Header.DopplerNumberOfBins > 0)
        {
            blockHeader["VEL"] = new VariableDataBlock(new VariableBlock(
                msg1: msg1Header,
                scale: velScale,
                offset: 129.0f,
                pointer: msg1Header.VelocityPointer
            ));
            blockHeader["SW"] = new VariableDataBlock(new VariableBlock(
                msg1: msg1Header,
                scale: 2.0f,
                offset: 192.0f,
                pointer: msg1Header.WidthPointer
            ));
        }

        return blockHeader;
    }

    /// <summary>
    /// Extracts altitude, range start, range scale, and timestamp from a sweep's data header.
    /// </summary>
    private (double altitude, short rangeStart, short rangeScale, DateTime timestamp) ExtractSweepMetadata(SweepData sweep)
    {
        double? altitude = null;
        short? rangeStart = null;
        short? rangeScale = null;
        DateTime? timestamp = null;

        var dataHeader = sweep.Message31DataHeader;
        // Extract altitude from VOL block
        if (dataHeader.TryGetValue("VOL", out var volBlockData) && volBlockData is ConstantDataBlock { Volume: not null } volConstant)
        {
            altitude = volConstant.Volume.Height + volConstant.Volume.FeedhornHeight;
        }

        // Extract range start and scale from first VariableBlock
        var variableBlocks = dataHeader
            .Where(kvp => kvp.Value is VariableDataBlock)
            .Select(kvp => (kvp.Key, ((VariableDataBlock)kvp.Value).Variable))
            .ToList();

        if (variableBlocks.Count > 0)
        {
            var firstBlock = variableBlocks[0];
            rangeStart = firstBlock.Item2.FirstGate;
            rangeScale = firstBlock.Item2.GateSpacing;
        }

        // Extract timestamp from Message31Header
        var msg31 = sweep.Message31Header;
        Console.WriteLine($"Message31Header: {msg31.CollectDate} {msg31.CollectMilliseconds}");
        var timeMs = ((long)(msg31.CollectDate - 1)) * 86400000L + msg31.CollectMilliseconds;
        timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timeMs).DateTime;
        if (altitude == null || rangeStart == null || rangeScale == null || timestamp == null)
        {
            throw new InvalidOperationException("Failed to extract sweep metadata");
        }

        return (altitude.Value, rangeStart.Value, rangeScale.Value, timestamp.Value);
    }

    /// <summary>
    /// Read sweep data (moments) into float[,], shaped (nRays, nGates).
    /// Applies simple scale/offset: value = (raw - offset) / scale.
    /// </summary>
    private Dictionary<string, float[,]> ReadSweepData(SweepData sweep)
    {
        var result = new Dictionary<string, float[,]>();

        var dataHeader = sweep.Message31DataHeader;

        var startRecord = sweep.RecordNumber;
        var endRecord = sweep.RecordEnd ?? startRecord;

        // Collect intermediate record numbers to skip
        var skipRecords = new HashSet<int>();
        if (sweep.IntermediateRecords != null)
        {
            skipRecords.UnionWith(sweep.IntermediateRecords);
        }

        // Identify moments (exclude constant blocks)
        var momentEntries = dataHeader
            .Where(kvp => kvp.Key != "VOL" && kvp.Key != "ELV" && kvp.Key != "RAD" && kvp.Value is VariableDataBlock)
            .ToDictionary(kvp => kvp.Key, kvp => ((VariableDataBlock)kvp.Value).Variable);

        static string MapMomentName(string key)
        {
            var trimmed = key.TrimEnd();
            return _nexradMapping.TryGetValue(trimmed, out var mapped) ? mapped : trimmed;
        }

        // Prepare per-moment storage
        var momentRows = momentEntries.ToDictionary(
            kvp => MapMomentName(kvp.Key),
            kvp => new List<float[]>()
        );

        for (int rec = startRecord; rec <= endRecord; rec++)
        {
            if (skipRecords.Contains(rec)) continue;
            if (!InitRecord(rec)) break;

            // choose reader for current record
            BinaryReader? tempReader = null;
            BinaryReader reader = this;
            bool created = false;
            if (IsCompressed())
            {
                var ldmKey = GetLdm(rec);
                if (_localDataManager.TryGetValue(ldmKey, out var ldmData))
                {
                    var ms = new MemoryStream(ldmData);
                    tempReader = new BinaryReader(ms);
                    reader = tempReader;
                    created = true;
                }
            }

            foreach (var (key, header) in momentEntries)
            {
                var mappedKey = MapMomentName(key);
                var ngates = header.NumberOfGates;
                if (ngates == 0) continue;
                var wordSize = header.WordSize;
                var scale = header.Scale;
                var offset = header.Offset;
                Console.WriteLine($"{mappedKey} Scale: {scale}, Offset: {offset}");
                var dataOffset = header.DataOffset;

                // Position to data offset (relative to record start)
                long absolutePos;
                if (dataOffset >= _recordStart && dataOffset < _recordStart + _recordSize)
                {
                    absolutePos = dataOffset; // already absolute
                }
                else
                {
                    absolutePos = _recordStart + dataOffset; // relative
                }

                reader.BaseStream.Position = absolutePos;

                var ray = new float[ngates];
                if (wordSize == 8) // 8-bit unsigned integer
                {
                    var bytes = reader.ReadBytes(ngates);
                    for (int i = 0; i < ngates; i++)
                        ray[i] = (bytes[i] - offset) / scale;

                }
                else
                {
                    // 16-bit big-endian
                    for (int i = 0; i < ngates; i++)
                    {
                        var b1 = reader.ReadByte();
                        var b2 = reader.ReadByte();
                        var raw = (ushort)((b1 << 8) | b2);
                        ray[i] = (raw - offset) / scale;
                    }
                }
                momentRows[mappedKey].Add(ray);
            }

            if (created && tempReader != null)
            {
                tempReader.Dispose();
            }
        }

        // Convert lists to 2D arrays
        foreach (var kvp in momentRows)
        {
            var rows = kvp.Value;
            if (rows.Count == 0) continue;
            var ngates = rows[0].Length;
            var arr = new float[rows.Count, ngates];
            for (int r = 0; r < rows.Count; r++)
            {
                var src = rows[r];
                for (int c = 0; c < ngates; c++)
                {
                    arr[r, c] = src[c];
                }
            }
            result[kvp.Key] = arr;
        }

        return result;
    }


    /// <summary>
    /// Extract metadata (elevations, quantities, coordinates) from file headers without reading sweep data.
    /// This is much faster than loading the full file.
    /// </summary>
    public static List<ScanMetadata> ExtractMetadata(NexradLevel2Reader reader)
    {
        var result = new List<ScanMetadata>();

        // Get data headers (this doesn't read the actual sweep data)
        var (_, msg31Header, msg31DataHeader) = reader.DataHeaders;
        if (msg31DataHeader.Count == 0) return result;


        // Extract site coordinates from VOL block in first sweep
        double lat = 0, lon = 0, height = 0;
        DateTime timestamp = DateTime.MinValue;

        var firstSweep = msg31DataHeader[0];
        var firstDataHeader = firstSweep.Message31DataHeader;
        if (firstDataHeader.TryGetValue("VOL", out var volBlockData) && volBlockData is ConstantDataBlock { Volume: not null } volConstant)
        {
            lat = volConstant.Volume.Latitude;
            lon = volConstant.Volume.Longitude;
            height = volConstant.Volume.Height + volConstant.Volume.FeedhornHeight;
        }

        // Extract timestamp from first message header
        var msg31 = firstSweep.Message31Header;
        var timeMs = ((long)(msg31.CollectDate - 1)) * 86400000L + msg31.CollectMilliseconds;
        timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timeMs).DateTime;

        // Extract metadata for each sweep
        for (int sweepNum = 0; sweepNum < msg31DataHeader.Count; sweepNum++)
        {
            var sweep = msg31DataHeader[sweepNum];

            // Get elevation from message headers
            if (sweepNum >= msg31Header.Count || msg31Header[sweepNum] == null || msg31Header[sweepNum].Count == 0)
            {
                continue;
            }
            if (msg31Header[sweepNum][0] is not Message31Header header)
            {
                continue;
            }
            double elevation = header.ElevationAngle;

            // Extract quantities from variable blocks and map to CfRadial2/ODIM names

            var quantities = new List<string>();
            var dataHeader = sweep.Message31DataHeader;
            foreach (var kvp in dataHeader)
            {
                var blockName = kvp.Key;
                // Variable blocks are REF, VEL, SW, ZDR, PHI, RHO, CFP
                if (kvp.Value is VariableDataBlock)
                {
                    // Map NEXRAD names to CfRadial2/ODIM names
                    if (_nexradMapping.ContainsKey(blockName))
                    {
                        quantities.Add(_nexradMapping[blockName]);
                    }
                    else
                    {
                        quantities.Add(blockName);
                    }
                }
            }

            result.Add(new ScanMetadata
            {
                Elevation = elevation,
                Quantities = quantities,
                Latitude = lat,
                Longitude = lon,
                Height = height,
                Timestamp = timestamp
            });
        }

        return result;
    }

    /// <summary>
    /// Extract instrument name from MSG_31 header.
    /// The id field in the MSG_31 header contains the radar identifier.
    /// </summary>
    private string ExtractInstrumentName()
    {
        try
        {
            // Get data headers to access MSG_31 headers
            var (_, msg31Header, _) = DataHeaders;

            if (msg31Header == null || msg31Header.Count == 0)
            {
                throw new InvalidDataException("No MSG_31 headers found in file");
            }

            // Get the first sweep's first MSG_31 header
            if (msg31Header[0] == null || msg31Header[0].Count == 0)
            {
                throw new InvalidDataException("No MSG_31 headers found in first sweep");
            }

            // Extract id from the first MSG_31 header
            if (msg31Header[0][0] is Message31Header firstHeader)
            {
                var id = firstHeader.Id?.TrimEnd('\0', ' ').ToUpperInvariant() ?? "";
                if (string.IsNullOrWhiteSpace(id))
                {
                    throw new InvalidDataException("Radar id in MSG_31 header is empty or invalid");
                }
                return id;
            }

            throw new InvalidDataException("First header in first sweep is not a Message31Header");
        }
        catch (Exception ex) when (ex is not InvalidDataException)
        {
            throw new InvalidDataException($"Failed to extract instrument name from MSG_31 header: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Extract scan name (VCP number).
    /// </summary>
    private string ExtractScanName()
    {
        // Would need to read msg_5 - for now return empty
        // In Python: f"VCP-{self.root.msg_5['pattern_number']}"
        return "";
    }
    /// <summary>
    /// Load sweep data arrays shaped (nRay, nBin) per variable.
    /// Returns Dictionary: sweepNumber -> { variableName -> float[,] }.
    /// </summary>
    private Dictionary<int, Dictionary<string, float[,]>> OpenSweepData(in IEnumerable<int>? sweeps = null)
    {
        var (_, _, msg31) = DataHeaders;

        // var sweepNumbers = sweeps != null && sweeps.Any() ? sweeps : [.. Enumerable.Range(0, msg31.Count)];

        var result = new Dictionary<int, Dictionary<string, float[,]>>();
        foreach (var i in sweeps ?? Enumerable.Range(0, msg31.Count))
        {
            if (i >= msg31.Count) continue;
            var sweep = msg31[i];
            var data = ReadSweepData(sweep);
            result[i] = data;
        }

        return result;
    }

    /// <summary>
    /// Writes sweep 0's DBZH (reflectivity) data as a PNG image.
    /// </summary>
    /// <param name="outputPath">Path where the PNG file will be saved</param>
    /// <param name="minDbz">Minimum dBZ value for normalization (default: -33)</param>
    /// <param name="maxDbz">Maximum dBZ value for normalization (default: 70)</param>
    public void WriteSweep0DbzhAsPng(string outputPath, float minDbz = -33f, float maxDbz = 70f)
    {
        var sweep = this[0];
        if (!sweep.TryGetValue("DBZH", out var dbzh))
        {
            throw new InvalidOperationException("DBZH data not found in sweep 0");
        }

        WriteDbzhAsPng(dbzh, outputPath, minDbz, maxDbz);
    }

    /// <summary>
    /// Writes DBZH (reflectivity) data as a PNG image.
    /// </summary>
    /// <param name="dbzh">The DBZH data array (rays x gates)</param>
    /// <param name="outputPath">Path where the PNG file will be saved</param>
    /// <param name="minDbz">Minimum dBZ value for normalization (default: -33)</param>
    /// <param name="maxDbz">Maximum dBZ value for normalization (default: 70)</param>
    public static void WriteDbzhAsPng(float[,] dbzh, string outputPath, float minDbz = -33f, float maxDbz = 70f)
    {
        var nRays = dbzh.GetLength(0);
        var nGates = dbzh.GetLength(1);

        // Create image (width = gates, height = rays)
        using var image = new Image<Rgb24>(nGates, nRays);

        // Normalize and convert to RGB
        var range = maxDbz - minDbz;
        if (range <= 0) range = 1f;

        for (int ray = 0; ray < nRays; ray++)
        {
            for (int gate = 0; gate < nGates; gate++)
            {
                var value = dbzh[ray, gate];

                // Handle NaN and out of range values
                if (float.IsNaN(value) || float.IsInfinity(value))
                {
                    image[gate, ray] = new Rgb24(0, 0, 0);
                    continue;
                }

                // Normalize to 0-1 range
                var normalized = Math.Clamp((value - minDbz) / range, 0f, 1f);

                // Apply weather radar colormap (blue -> green -> yellow -> red)
                var color = GetWeatherRadarColor(normalized);
                image[gate, ray] = color;
            }
        }

        // Save as PNG
        image.SaveAsPng(outputPath);
    }

    /// <summary>
    /// Gets a color from a weather radar colormap based on normalized value (0-1).
    /// Blue (low) -> Cyan -> Green -> Yellow -> Orange -> Red (high)
    /// </summary>
    private static Rgb24 GetWeatherRadarColor(float normalized)
    {
        if (normalized <= 0) return new Rgb24(0, 0, 0);
        if (normalized >= 1) return new Rgb24(255, 0, 0);

        // Weather radar colormap
        if (normalized < 0.2f)
        {
            // Blue to Cyan
            var t = normalized / 0.2f;
            return new Rgb24(0, (byte)(t * 255), 255);
        }
        else if (normalized < 0.4f)
        {
            // Cyan to Green
            var t = (normalized - 0.2f) / 0.2f;
            return new Rgb24(0, 255, (byte)((1 - t) * 255));
        }
        else if (normalized < 0.6f)
        {
            // Green to Yellow
            var t = (normalized - 0.4f) / 0.2f;
            return new Rgb24((byte)(t * 255), 255, 0);
        }
        else if (normalized < 0.8f)
        {
            // Yellow to Orange
            var t = (normalized - 0.6f) / 0.2f;
            return new Rgb24(255, (byte)((1 - t * 0.5f) * 255), 0);
        }
        else
        {
            // Orange to Red
            var t = (normalized - 0.8f) / 0.2f;
            return new Rgb24(255, (byte)((0.5f - t * 0.5f) * 255), 0);
        }
    }

    #endregion
}
