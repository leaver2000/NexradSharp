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
        NexradLevel2Volume volume;
        volume = reader[..];
        Console.WriteLine(volume);

        Assert.True(volume.Count == 10);

        foreach (var (sweepIndex, sweep) in volume)
        {
            Assert.IsType<int>(sweepIndex);
            // assert is subclass of Radar.Sweep<Radar.Field>
            Assert.IsAssignableFrom<Radar.Sweep<Radar.Field>>(sweep);
            Assert.IsType<NexradLevel2Sweep>(sweep);
            foreach (var (fieldName, field) in sweep)
            {
                Assert.IsType<FieldName>(fieldName);
                // Assert.IsType<Radar.Field>(field.Compute());
            }
        }
        volume = reader[0..1];
        Assert.Single(volume);
        NexradLevel2Sweep s = volume[0];
        Assert.Equal(0, s.ScanIndex);



        // Assert.True(sweep.Count == 8);
        // Assert.True(volume.Keys.Count == 1);
        // Assert.True(volume.Values.Count == 1);
        // Assert.True(volume.Keys.First() == 0);
        // Assert.True(volume.Values.First() is NexradLevel2Sweep);
        // Assert.True(volume.Values.First().Count == 1);
        // Assert.True(volume.Values.First().Keys.Count == 1);
        // Assert.True(volume.Values.First().Values.Count == 1);








        // Assert
        Assert.NotNull(volume);
        Assert.True(volume.Count > 0);
        Assert.Contains(0, volume.Keys);
    }
}
