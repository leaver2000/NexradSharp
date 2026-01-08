using System;
using System.IO;
using NexradSharp;
using Xunit;

namespace NexradSharp.Tests;




public class NexradLevel2ReaderAccessorTests
{
    string solutionRoot => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
    string filePath => Path.Combine(solutionRoot, "KLSX20251229_065156_V06");

    [Fact]
    public void GetSweepsAccessor_ReturnsAccessorWithKeys()
    {
        // Arrange
        using var reader = NexradLevel2Reader.Open(filePath);

        // Act
        var volume = reader[..];
        foreach (var sweep in volume)
        {
            // Console.WriteLine(sweep.ElevationAngle);
            Console.WriteLine(" TS | EL | Alt | start | scale | Key | NRays | NBins");
            foreach (var (k, v) in sweep)
            {
                Console.WriteLine($"{sweep.Timestamp:yyyy-MM-dd HH:mm:ss} {sweep.ElevationAngle:F2}  {sweep.Altitude}   {sweep.RangeStart}     {sweep.RangeScale}    {k} {v.GetLength(0)} {v.GetLength(1)}");
            }
        }


        // Assert
        Assert.NotNull(volume);
        Assert.True(volume.Count > 0);
        Assert.Contains(0, volume.Keys);
    }


    [Fact]
    public void WriteSweep0DbzhAsPng_CreatesImageFile()
    {
        if (!File.Exists(filePath)) return;

        // Arrange
        using var reader = NexradLevel2Reader.Open(filePath);
        var outputPath = Path.Combine(solutionRoot, "sweep0_dbzh.png");

        // Act
        reader.WriteSweep0DbzhAsPng(outputPath);

        // Assert
        Assert.True(File.Exists(outputPath), $"PNG file should be created at {outputPath}");
        var fileInfo = new FileInfo(outputPath);
        Assert.True(fileInfo.Length > 0, "PNG file should not be empty");

        // Get dimensions from the sweep data
        var sweep = reader[0];
        var dbzh = sweep["DBZH"];

        // Note: File is kept for manual inspection - delete manually if needed
        Console.WriteLine($"PNG file created at: {outputPath}");
        Console.WriteLine($"File size: {fileInfo.Length} bytes");
        Console.WriteLine($"Image dimensions: {dbzh.GetLength(1)} x {dbzh.GetLength(0)} (width x height, gates x rays)");
    }
}
