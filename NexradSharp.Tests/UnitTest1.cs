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
                Console.WriteLine(sweep);
            }
        }


        // Assert
        Assert.NotNull(volume);
        Assert.True(volume.Count > 0);
        Assert.Contains(0, volume.Keys);
    }
}
