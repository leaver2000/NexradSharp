namespace NexradSharp;

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
public enum FieldName
{
    DBZH,
    VRADH,
    WRADH,
    ZDR,
    PHIDP,
    RHOHV,
    CCORH,
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
    METADATA = 0,
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
