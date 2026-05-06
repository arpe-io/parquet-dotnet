using System.Numerics;
using BenchmarkDotNet.Attributes;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks;

[ShortRunJob]
[MeanColumn]
[MemoryDiagnoser]
[MarkdownExporter]
public class BigDecimalBenchmarks {

    private BigDecimal[] _data = null!;
    private ParquetSchema _schema = null!;
    private SchemaElement _tse = null!;

    [Params(1_000, 100_000, 1_000_000)]
    public int Size;

    [Params(10, 38)]
    public int Precision;

    [GlobalSetup]
    public void Setup() {
        int scale = Precision / 3;
        _schema = new ParquetSchema(new BigDecimalDataField("bd", Precision, scale));

        _tse = new SchemaElement {
            Name = "bd",
            Type = Meta.Type.FIXED_LEN_BYTE_ARRAY,
            Precision = Precision,
            Scale = scale,
            TypeLength = BigDecimal.GetBufferSize(Precision)
        };

        var rng = new Random(42);
        _data = new BigDecimal[Size];
        BigInteger maxVal = BigInteger.Pow(10, Precision) - 1;

        for(int i = 0; i < Size; i++) {
            // Mix of positive, negative, zero
            BigInteger unscaled;
            if(i % 100 == 0) {
                unscaled = BigInteger.Zero;
            } else {
                // Generate a random value within precision range
                byte[] bytes = new byte[BigDecimal.GetBufferSize(Precision)];
                rng.NextBytes(bytes);
                unscaled = new BigInteger(bytes);
                // Clamp to valid range
                unscaled = BigInteger.Remainder(unscaled, maxVal);
            }
            _data[i] = new BigDecimal(unscaled, Precision, scale);
        }
    }

    [Benchmark(Description = "BigDecimal Encode (write)")]
    public void Encode() {
        using var ms = new MemoryStream();
        ParquetPlainEncoder.Encode(_data.AsSpan(), ms, _tse);
    }

    [Benchmark(Description = "BigDecimal GetBytes per element")]
    public void GetBytesPerElement() {
        for(int i = 0; i < _data.Length; i++) {
            _ = _data[i].GetBytes();
        }
    }

    [Benchmark(Description = "BigDecimal WriteBytes per element")]
    public void WriteBytesPerElement() {
        Span<byte> buffer = stackalloc byte[16];
        for(int i = 0; i < _data.Length; i++) {
            _data[i].WriteBytes(buffer);
        }
    }

    [Benchmark(Description = "BigDecimal full write to parquet")]
    public async Task FullParquetWrite() {
        using var ms = new MemoryStream();
        await using ParquetWriter pw = await ParquetWriter.CreateAsync(
            _schema, ms, new ParquetOptions { CompressionMethod = CompressionMethod.None });
        using ParquetRowGroupWriter rgw = pw.CreateRowGroup();
        await rgw.WriteAsync<BigDecimal>(_schema.DataFields[0], _data);
    }
}
