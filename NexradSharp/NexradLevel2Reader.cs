using System.Buffers.Binary;
using System.Text;
using SharpCompress.Compressors.BZip2;


namespace NexradSharp;

// ================================================================================================================= //
#region constructor methods
// ================================================================================================================= //
/// <summary>
/// Opens sweeps from the NEXRAD file and returns them as a NexradLevel2Volume for easy key-based access.
/// </summary>
/// <param name="sweeps">Optional list of sweep numbers to extract. If null, extracts all sweeps.</param>
/// <returns>A NexradLevel2Volume that provides key-based access to sweep data</returns>
/// <example>
/// <code>
/// using var reader = NexradLevel2Reader.Open("KLSX20251229_065156_V06");
/// var sweeps = reader[..];
/// var sweep0 = sweeps[0]; // or just reader[0]
/// var reflectivityArray = sweep0[FieldName.DBZH]; // DataArray with data and attributes
/// var reflectivity = reflectivityArray.Data; // Span2D&lt;ushort&gt; with raw quantized data
/// var (scale, offset) = reflectivityArray.Attributes; // Scale/offset for dequantization
/// // Or use convenience methods:
/// var (scale2, offset2) = sweep0.GetScaleOffset(FieldName.DBZH);
/// </code>
/// </example>
/// <param name="input">The stream to read the NEXRAD file from</param>
/// <param name="leaveOpen">Whether to leave the stream open after the reader is disposed</param>
public class NexradLevel2Reader(Stream input, bool leaveOpen = false) : BinaryReader(input, Encoding.ASCII, leaveOpen)
{
    public static NexradLevel2Reader Open(string fileName, bool leaveOpen = false)
    {
        if (!File.Exists(fileName)) throw new FileNotFoundException(fileName);
        return new NexradLevel2Reader(File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read), leaveOpen);
    }
    #endregion
    // ============================================================================================================= //
    #region Properties
    // ============================================================================================================= //
    private (List<MessageHeader> dataHeader, List<List<RadarDataHeader>> messagesHeader, List<SweepData> messagesDataHeader)? _headers = null;
    public (List<MessageHeader> dataHeader, List<List<RadarDataHeader>> messagesHeader, List<SweepData> messagesDataHeader) Headers
    {
        get
        {
            _headers ??= GetHeaders();
            return _headers.Value;
        }
    }
    public List<MessageHeader> DataHeader => Headers.dataHeader;
    public List<List<RadarDataHeader>> Message31Headers => Headers.messagesHeader;
    public List<SweepData> Message31DataHeaders => Headers.messagesDataHeader;
    #endregion

    // ============================================================================================================= //
    #region public methods
    // ============================================================================================================= //
    public NexradLevel2Sweep this[int index]
    {
        get
        {
            var elangle = ElevationAngles[index];
            var sweep = Message31DataHeaders[index];

            var data = ReadSweepData(sweep);
            var (altitude, rangeStart, rangeScale, datetime) = ExtractSweepMetadata(sweep);
            return new NexradLevel2Sweep(data, index, 0.0, elangle, rangeScale, rangeStart);
        }
    }

    public NexradLevel2Volume this[IEnumerable<int> indices]
    {
        get
        {
            ArgumentNullException.ThrowIfNull(indices);
            var sweepData = OpenSweepData(indices);
            var indicesList = indices.ToList();
            var metadata = indices.ToDictionary(
                index => index,
                index => ExtractSweepMetadata(Message31DataHeaders[index])
            );

            // Extract volume-level metadata from first sweep
            var firstIndex = indicesList[0];
            var firstSweep = Message31DataHeaders[firstIndex];
            var (altitude, _, _, datetime) = ExtractSweepMetadata(firstSweep);
            var lat = firstSweep.ConstantBlock.Volume.Latitude;
            var lon = firstSweep.ConstantBlock.Volume.Longitude;

            var sweepRangeInfo = indicesList.ToDictionary(
                index => index,
                index =>
                {
                    var (_, rangeStart, rangeScale, _) = metadata[index];
                    return (rangeStart, rangeScale);
                }
            );

            // Extract start azimuth angles from first radial of each sweep
            var startAzimuths = indicesList.Select(index =>
            {
                var sweep = Message31DataHeaders[index];
                return (double)sweep.RadarDataHeader.AzimuthAngle;
            }).ToList();

            return new NexradLevel2Volume(sweepData, datetime, altitude, lat, lon, ElevationAngles, startAzimuths, sweepRangeInfo);
        }
    }
    public NexradLevel2Volume this[Range range]
    {
        get
        {

            var messages = Message31DataHeaders;
            Dictionary<int, Dictionary<FieldName, Radar.Field>>? sweepData;
            List<int> sweepIndices;
            if (range.Equals(..))
            {
                sweepIndices = [.. Enumerable.Range(0, messages.Count)];
                sweepData = OpenSweepData(null);
            }
            else
            {
                sweepIndices = [.. Enumerable.Range(range.Start.Value, range.End.Value)];
                sweepData = OpenSweepData(sweepIndices);
            }

            var elevationAngles = ElevationAngles;
            var metadata = sweepIndices.ToDictionary(
                index => index,
                index => ExtractSweepMetadata(messages[index])
            );

            // Extract volume-level metadata from first sweep

            var firstSweep = messages[0];
            var (altitude, _, _, datetime) = ExtractSweepMetadata(firstSweep);
            var lat = firstSweep.ConstantBlock.Volume.Latitude;
            var lon = firstSweep.ConstantBlock.Volume.Longitude;

            var sweepRangeInfo = sweepIndices.ToDictionary(
                index => index,
                index =>
                {
                    var (_, rangeStart, rangeScale, _) = metadata[index];
                    return (rangeStart, rangeScale);
                }
            );

            // Extract start azimuth angles from first radial of each sweep
            var startAzimuths = sweepIndices.Select(index =>
            {
                var sweep = messages[index];
                return (double)sweep.RadarDataHeader.AzimuthAngle;
            }).ToList();

            return new NexradLevel2Volume(sweepData, datetime, altitude, lat, lon, elevationAngles, startAzimuths, sweepRangeInfo);
        }
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
            var messages = Message31Headers;
            var elevations = new List<double>(messages.Count);

            foreach (var sweepHeaders in messages)
            {
                if (sweepHeaders.Count > 0)
                {
                    var firstHeader = sweepHeaders[0];
                    if (firstHeader is RadarDataHeader header) { elevations.Add(header.ElevationAngle); }
                    else { elevations.Add(double.NaN); }
                }
                else { elevations.Add(double.NaN); }
            }

            return elevations;
        }
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


    private static readonly Dictionary<DataName, FieldName> _nexradMapping = new()
    {
        [DataName.REF] = FieldName.DBZH,
        [DataName.VEL] = FieldName.VRADH,
        [DataName.SW] = FieldName.WRADH,
        [DataName.ZDR] = FieldName.ZDR,
        [DataName.PHI] = FieldName.PHIDP,
        [DataName.RHO] = FieldName.RHOHV,
        [DataName.CFP] = FieldName.CCORH,
    };
    // NEXRAD Level II file structures and sizes
    // The details on these structures are documented in:
    // "Interface Control Document for the Archive II/User" RPG Build 12.0
    // Document Number 2620010E and
    // "Interface Control Document for the RDA/RPG" Open Build 13.0
    // Document Number 2620002M
    // NEXRAD Level II constants
    private const int RECORD_BYTES = 2432;
    /// <summary>
    /// The contents of the message header along with the seven (7) message types contained in the Archive II file are briefly described in this ICD. The Archive II raw data format contains a 28-byte header. The first 12 bytes are empty, which means the "Message Size" does not begin until byte 13 (halfword 7 or full word 4). This 12 byte offset is due to legacy compliance (previously known as the "CTM header"). See the RDA/RPG ICD for more details (Message Header Data).
    /// </summary>
    private const int CTM_HEADER_OFFSET = 12;

    private const int CONTROL_WORD_SIZE = 4;

    // # Table II Message Header Data
    // Message header sizes are now calculated using struct SizeOf properties
    // MessageHeader.SizeOf = 16 bytes
    // RadarDataHeader has variable size due to BlockPointers array, calculated as: 4 + 4 + 2 + 2 + 4 + 1 + 1 + 2 + 1 + 1 + 1 + 1 + 4 + 1 + 1 + 2 + (10 * 4) = 72 bytes
    // Helper methods for reading big-endian values
    private static uint ReadUInt32BE(BinaryReader br)
    {
        var bytes = br.ReadBytes(4);
        return (uint)((bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3]);
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

        int count;
        var buffer = new byte[4096];
        while ((count = Read(buffer, 0, buffer.Length)) > 0)
            fileBytes.AddRange(buffer.Take(count));

        BaseStream.Position = savedPos;

        // Search for bzip2 magic: BZhX1AY&SY (where X is 0-9)
        // Python: seq = np.array([66, 90, 104, 0, 49, 65, 89, 38, 83, 89], dtype=np.uint8)
        // Python: rd = util.rolling_dim(self._fh, len(seq))
        // Python: self._bz2_indices = np.nonzero((rd == seq).sum(1) >= 9)[0] - 4
        // This means: find positions where at least 9 out of 10 bytes match the pattern
        // Pattern: [66, 90, 104, X, 49, 65, 89, 38, 83, 89] where X can be any byte
        byte[] pattern = [66, 90, 104, 0, 49, 65, 89, 38, 83, 89]; // X is wildcard (position 3)

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


        var compressed = ReadBytes((int)bz2Size); // Read compressed data


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
                var size = BinaryPrimitives.ReadUInt16BigEndian(ReadBytes(2));

                var msgType = ReadMessageType();
                BaseStream.Position = start;

                var recordSize = size * 2 + 12;
                if (msgType != MessageType.GENERIC_FORMAT && recordSize < RECORD_BYTES)
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
    private MessageType ReadMessageType() => (MessageType)ReadByte();

    /// <summary>
    /// Read and unpack message header.
    /// Python equivalent: get_message_header()
    /// Note: Must call InitRecord() first to position at the start of a record
    /// </summary>
    public MessageHeader GetMessageHeader()
    {
        BinaryReader? tempReader = null;
        BinaryReader reader = this;

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
                reader = tempReader;
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
        if (tempReader != null)
        {
            reader.BaseStream.Position = _recordStart + _recordPos;
        }
        else
        {
            BaseStream.Position = _recordStart + _recordPos;
        }

        // Read MSG_HEADER structure (16 bytes, big-endian)
        var header = MessageHeader.Read(reader);
        _recordPos += MessageHeader.SizeOf; // Update position after reading header

        // Clean up temporary reader if we created one
        tempReader?.Dispose();

        return header;
    }


    /// <summary>
    /// Get metadata header.
    /// Python equivalent: get_metadata_header()
    /// </summary>
    public Dictionary<MessageType, List<MessageHeaderWithMetadata>> GetMetadataHeader()
    {
        // """Get metadaata header"""
        // # data offsets
        // # ICD 2620010J
        // # 7.3.5 Metadata Record
        // # the above document will evolve over time
        // # please revisit and adopt accordingly
        var meta_headers = new Dictionary<MessageType, List<MessageHeaderWithMetadata>>();
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
            if (message_header.Type == MessageType.METADATA)
            {
                rec += 1;
                continue;
            }

            // stop if first non meta header is found
            // Metadata types: 2 (RDA_STATUS_DATA), 3 (PERFORMANCE_MAINTENANCE_DATA), 5 (VOLUME_COVERAGE_PATTERN_RDA),
            // 13 (CLUTTER_FILTER_BYPASS_MAP), 15 (CLUTTER_FILTER_MAP), 18 (RDA_ADAPTATION_DATA)
            var msgType = (MessageType)message_header.Type;
            if (msgType is not (
                MessageType.RDA_STATUS_DATA or MessageType.PERFORMANCE_MAINTENANCE_DATA
                or MessageType.VOLUME_COVERAGE_PATTERN_RDA or MessageType.CLUTTER_FILTER_BYPASS_MAP
                or MessageType.CLUTTER_FILTER_MAP or MessageType.RDA_ADAPTATION_DATA
            )) break;

            // Create header with metadata
            var headerWithMeta = message_header.With(
                rec,                // RecordNumber
                filepos             // FilePosition
            );

            // Add to appropriate message type list
            if (!meta_headers.TryGetValue(msgType, out List<MessageHeaderWithMetadata>? value))
            {
                value = [];
                meta_headers[msgType] = value;
            }

            value.Add(headerWithMeta);

            rec += 1;
        }

        return meta_headers;
    }

    private Dictionary<MessageType, List<MessageHeaderWithMetadata>>? _metaHeader = null;

    /// <summary>
    /// Get metadata header (cached).
    /// Python equivalent: self.meta_header property
    /// </summary>
    public Dictionary<MessageType, List<MessageHeaderWithMetadata>> MetaHeader
    {
        get
        {
            _metaHeader ??= GetMetadataHeader();
            return _metaHeader;
        }
    }

    /// <summary>
    /// Load all data header from file.
    /// Python equivalent: get_data_header()
    /// Returns: (data_header, _msg_31_header, _msg_31_data_header)
    /// </summary>
    public (List<MessageHeader> dataHeader, List<List<RadarDataHeader>> messagesHeader, List<SweepData> messagesDataHeader) GetHeaders()
    {
        // get the record number from the meta header
        // Find the last metadata record (typically RDA_STATUS_DATA, but could be any metadata type)
        var metaHeader = MetaHeader;
        if (metaHeader.Count == 0)
        {
            throw new InvalidOperationException("No metadata headers found");
        }

        // Find the metadata record with the highest record number (last one)
        MessageHeaderWithMetadata? lastMetaRecord = null;
        foreach (var kvp in metaHeader)
        {
            foreach (var record in kvp.Value)
            {
                if (lastMetaRecord == null || record.RecordNumber > lastMetaRecord.Value.RecordNumber)
                {
                    lastMetaRecord = record;
                }
            }
        }

        if (lastMetaRecord == null)
        {
            throw new InvalidOperationException("No valid metadata records found");
        }

        var recordNumber = lastMetaRecord.Value.RecordNumber;
        if (!InitRecord(recordNumber))
        {
            throw new InvalidOperationException($"Failed to initialize record {recordNumber}");
        }

        int currentSweep = -1;
        int currentHeader = -1;
        var sweepMessageHeaders = new List<RadarDataHeader>();
        var sweepIntermediateRecords = new List<int>();

        var dataHeader = new List<MessageHeader>();
        var messagesHeader = new List<List<RadarDataHeader>>();
        var messagesDataHeader = new List<SweepData>();

        int maxRecords = 10000; // Safety limit
        int recordCount = 0;

        // Process records starting from the one after the last metadata record
        while (recordCount < maxRecords && InitNextRecord())
        {
            recordCount++;
            currentHeader += 1;
            // get message headers
            var msgHeader = GetMessageHeader();

            // keep all data headers
            dataHeader.Add(msgHeader);


            if (msgHeader.Type == MessageType.GENERIC_FORMAT)                                                           // Generic Radar Data Format
            {
                // Read the full message header (MSG_31)
                var messagesHdr = ReadRadarDataHeader();

                // retrieve data/const headers from msg 31
                // check if this is a new sweep
                SweepStatus status = messagesHdr.RadialStatus;



                if (status == SweepStatus.INTERMEDIATE_RADIAL) {/** 1 - intermediate radial pass */}                    // Intermediate Radial Pass
                else if (status == SweepStatus.END_ELEVATION || status == SweepStatus.END_VOLUME)
                {
                    // 2 - end of elevation
                    // 4 - end of volume
                    if (messagesDataHeader.Count > 0)
                    {
                        var lastSweep = messagesDataHeader[^1];
                        messagesDataHeader[^1] = lastSweep with
                        {
                            RecordEnd = _recordNumber,
                            IntermediateRecords = sweepIntermediateRecords.Count > 0 ? sweepIntermediateRecords : null
                        };
                    }
                    // Note: In Python this is self._data[current_sweep] = sweep
                    // but we're not storing it in a class field, just in the return value
                    messagesHeader.Add(sweepMessageHeaders);
                }
                else if (status == SweepStatus.NEW_ELEVATION || status == SweepStatus.NEW_VOLUME || status == SweepStatus.NEW_ELEVATION_LAST_ELEVATION_IN_VCP)
                {
                    // 0 - start of new elevation
                    // 3 - start of new volume
                    // 5 - start of new elevation, last elevation in VCP
                    currentSweep += 1;
                    // create new sweep object
                    sweepMessageHeaders = [];
                    sweepIntermediateRecords = [];

                    // new message 31 data

                    var (constantBlock, variableBlocks) = ParseRadarDataBlocks(messagesHdr);
                    messagesDataHeader.Add(new SweepData(
                        RecordNumber: _recordNumber,
                        FilePosition: _recordStart,
                        RadarDataHeader: messagesHdr,
                        MessageType: msgHeader.Type,
                        ConstantBlock: constantBlock,
                        VariableBlocks: variableBlocks
                    ));


                }

                sweepMessageHeaders.Add(messagesHdr);
            }
            else
            {
                sweepIntermediateRecords.Add(_recordNumber);
            }
        }

        // Ensure the last sweep is finalized if not already closed by status 2/4
        if (messagesDataHeader.Count > 0)
        {
            var lastSweep = messagesDataHeader[^1];
            if (lastSweep.RecordEnd == null)
            {
                messagesDataHeader[^1] = lastSweep with
                {
                    RecordEnd = _recordNumber,
                    IntermediateRecords = sweepIntermediateRecords.Count > 0 ? sweepIntermediateRecords : null
                };
            }
            messagesHeader.Add(sweepMessageHeaders);
        }

        return (dataHeader, messagesHeader, messagesDataHeader);
    }

    /// <summary>
    /// Read MSG_31 header after the message header.
    /// Python equivalent: _unpack_dictionary(self._rh.read(msg_len, width=1), msg, ...)
    /// </summary>
    private RadarDataHeader ReadRadarDataHeader()
    {
        BinaryReader? tempReader = null;
        BinaryReader reader = this;

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
                reader = tempReader;
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

        // Only RadarData is supported
        var header = RadarDataHeader.Read(reader);
        _recordPos += RadarDataHeader.SizeOf;

        // Clean up temporary reader if we created one
        tempReader?.Dispose();

        return header;
    }

    /// <summary>
    /// Parse data blocks from Message 31.
    /// Python equivalent: parsing block_pointers and DATA_BLOCK_HEADER
    /// </summary>
    private (ConstantBlock constantBlock, Dictionary<DataName, VariableBlock> variableBlocks) ParseRadarDataBlocks(RadarDataHeader header)
    {
        var variables = new Dictionary<DataName, VariableBlock>();

        var blockPointers = header.BlockPointers.Where(bp => bp > 0).ToArray();
        var nBlocks = Math.Min(blockPointers.Length, header.BlockCount);

        BinaryReader? tempReader = null;
        BinaryReader reader = this;

        // For compressed files, use decompressed data
        if (IsCompressed())
        {
            var ldmKey = GetLdm(_recordNumber);
            if (_localDataManager.TryGetValue(ldmKey, out byte[]? ldmData))
            {
                var ms = new MemoryStream(ldmData);
                tempReader = new BinaryReader(ms);
                reader = tempReader;
            }
        }

        // Read the first 3 constant blocks (VOL, ELV, RAD) contiguously
        // The constant blocks are always first and contiguous: VOL, ELV, RAD
        if (nBlocks < 3)
            throw new InvalidOperationException("Expected at least 3 blocks (VOL, ELV, RAD)");

        // Read the first 3 constant blocks using their respective block pointers
        var constants = new ConstantBlock(reader, (blockPointers[0], blockPointers[1], blockPointers[2]));


        // Process variable blocks starting from index 3
        for (int i = 3; i < nBlocks; i++)
        {
            var targetPos = (long)blockPointers[i] + 12 + MessageHeader.SizeOf;
            reader.BaseStream.Position = _recordStart + targetPos;

            // Read DATA_BLOCK_HEADER (4 bytes: block_type (1 byte) + data_name (3 bytes))
            var blockType = (BlockType)reader.ReadByte();
            var name = Enum.Parse<DataName>(Encoding.ASCII.GetString(reader.ReadBytes(3)).TrimEnd('\0'));

            // All remaining blocks should be VARIABLE
            if (blockType != BlockType.VARIABLE)
                throw new InvalidOperationException($"Expected VARIABLE block at index {i}, but found {blockType}");

            // Variable block (REF, VEL, SW, ZDR, PHI, RHO, CFP)
            // Store data_offset relative to the start of the current record
            variables[name] = VariableBlock.Read(reader, reader.BaseStream.Position - _recordStart);
        }

        tempReader?.Dispose();

        return (constants, variables);
    }

    /// <summary>
    /// Extracts altitude, range start, range scale, and datetime from a sweep's data header.
    /// </summary>
    private static (double altitude, short rangeStart, short rangeScale, DateTime datetime) ExtractSweepMetadata(SweepData sweep)
    {
        // Extract altitude from VOL block
        var altitude = sweep.ConstantBlock.Volume.Height + sweep.ConstantBlock.Volume.FeedhornHeight;

        // Extract range start and scale from first VariableBlock
        var variableBlocks = sweep.VariableBlocks
            .Where(kvp => kvp.Value is VariableBlock)
            .Select(kvp => (kvp.Key, (VariableBlock)kvp.Value))
            .ToList();

        if (variableBlocks.Count == 0)
        {
            throw new InvalidOperationException("No variable blocks found in sweep data");
        }

        var (_, variable) = variableBlocks[0];
        var rangeStart = variable.FirstGate;
        var rangeScale = variable.GateSpacing;

        // Extract datetime from RadarDataHeader
        var messages = sweep.RadarDataHeader;
        var timeMs = ((long)(messages.CollectDate - 1)) * 86400000L + messages.CollectMilliseconds;
        var datetime = DateTimeOffset.FromUnixTimeMilliseconds(timeMs).DateTime;

        return (altitude, rangeStart, rangeScale, datetime);
    }

    /// <summary>
    /// Read sweep data (moments) into Span2D&lt;ushort&gt;, shaped (nRays, nGates).
    /// Returns raw quantized data without dequantization. Scale and offset are contained within each RadarField.
    /// </summary>
    private Dictionary<FieldName, Radar.Field> ReadSweepData(SweepData sweep)
    {
        var result = new Dictionary<FieldName, Radar.Field>();

        var dataHeader = sweep.VariableBlocks;

        var startRecord = sweep.RecordNumber;
        var endRecord = sweep.RecordEnd ?? startRecord;

        // Collect intermediate record numbers to skip
        var skipRecords = new HashSet<int>();
        if (sweep.IntermediateRecords != null)
            skipRecords.UnionWith(sweep.IntermediateRecords);



        // Identify moments (variable blocks only, constant blocks are stored separately)
        var momentEntries = dataHeader
            .Where(kvp => kvp.Value is VariableBlock)
            .ToDictionary(kvp => kvp.Key, kvp => (VariableBlock)kvp.Value);



        // Prepare per-moment storage for raw quantized values
        var momentRows = momentEntries.ToDictionary(
            kvp => _nexradMapping[kvp.Key],
            kvp => new List<ushort[]>()
        );

        // Store scale/offset for each moment (will be used when creating DataArray)
        var scaleOffsetMap = momentEntries.ToDictionary(
            kvp => _nexradMapping[kvp.Key],
            kvp => new Radar.ScaleOffset(kvp.Value.Scale, kvp.Value.Offset)
        );

        for (int rec = startRecord; rec <= endRecord; rec++)
        {
            if (skipRecords.Contains(rec)) continue;
            if (!InitRecord(rec)) break;

            // choose reader for current record
            BinaryReader? tempReader = null;
            BinaryReader reader = this;

            if (IsCompressed())
            {
                var ldmKey = GetLdm(rec);
                if (_localDataManager.TryGetValue(ldmKey, out var ldmData))
                {
                    var ms = new MemoryStream(ldmData);
                    tempReader = new BinaryReader(ms);
                    reader = tempReader;
                }
            }

            foreach (var (key, header) in momentEntries)
            {
                var mappedKey = _nexradMapping[key];
                var ngates = header.NumberOfGates;
                if (ngates == 0) continue;
                var wordSize = header.WordSize;
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

                var ray = new ushort[ngates];
                if (wordSize == 8) // 8-bit unsigned integer
                {
                    var bytes = reader.ReadBytes(ngates);
                    for (int i = 0; i < ngates; i++)
                        ray[i] = bytes[i]; // Store raw quantized value

                }
                else
                {
                    // 16-bit big-endian
                    for (int i = 0; i < ngates; i++)
                    {
                        var b1 = reader.ReadByte();
                        var b2 = reader.ReadByte();
                        ray[i] = (ushort)((b1 << 8) | b2); // Store raw quantized value
                    }
                }
                momentRows[mappedKey].Add(ray);
            }


            tempReader?.Dispose();

        }

        // Convert lists to Span2D and create RadarField instances with attributes
        foreach (var kvp in momentRows)
        {
            var rows = kvp.Value;
            if (rows.Count == 0) continue;
            var ngates = rows[0].Length;

            // Flatten 2D data into 1D array for Span2D
            var flatData = new List<ushort>();
            for (int r = 0; r < rows.Count; r++)
            {
                flatData.AddRange(rows[r]);
            }

            var span2D = new Span2D<ushort>(flatData, rows.Count, ngates);
            result[kvp.Key] = new Radar.Field(span2D, scaleOffsetMap[kvp.Key]);
        }

        return result;
    }



    /// <summary>
    /// Load sweep data arrays shaped (nRay, nBin) per variable.
    /// Returns Dictionary: index -> data where data is FieldName -> RadarField.
    /// </summary>
    private Dictionary<int, Dictionary<FieldName, Radar.Field>> OpenSweepData(in IEnumerable<int>? sweeps = null)
    {
        var messages = Message31DataHeaders;

        var result = new Dictionary<int, Dictionary<FieldName, Radar.Field>>();
        foreach (var i in sweeps ?? Enumerable.Range(0, messages.Count))
        {
            if (i >= messages.Count) continue;
            var sweep = messages[i];
            var data = ReadSweepData(sweep);
            result[i] = data;
        }

        return result;
    }
    #endregion
}