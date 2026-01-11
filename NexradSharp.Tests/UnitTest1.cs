using System;
using System.IO;
using System.Linq;
using NexradSharp;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
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

    /// <summary>
    /// Renders a Radar.Field as an image with dimensions (NRays, NBins).
    /// Each pixel at [ray, bin] corresponds to data[ray, bin].
    /// </summary>
    /// <param name="field">The radar field to render</param>
    /// <param name="outputPath">Path where the PNG image will be saved</param>
    internal static void RenderFieldAsImage(Radar.Field field, string outputPath)
    {
        var data = field.Data;
        int nrays = data.NRays;
        int nbins = data.NBins;

        // Find min and max values for normalization
        ushort min = ushort.MaxValue;
        ushort max = ushort.MinValue;
        for (int ray = 0; ray < nrays; ray++)
        {
            for (int bin = 0; bin < nbins; bin++)
            {
                var value = data[ray, bin];
                if (value < min) min = value;
                if (value > max) max = value;
            }
        }

        // Create image with width = nbins, height = nrays
        using var image = new Image<Rgb24>(nbins, nrays);

        // Map values to grayscale (0-255)
        float range = max > min ? max - min : 1;
        for (int ray = 0; ray < nrays; ray++)
        {
            for (int bin = 0; bin < nbins; bin++)
            {
                var value = data[ray, bin];
                byte gray = max > min ? (byte)(((value - min) / range) * 255) : (byte)0;
                image[bin, ray] = new Rgb24(gray, gray, gray);
            }
        }

        image.SaveAsPng(outputPath);
    }
}
