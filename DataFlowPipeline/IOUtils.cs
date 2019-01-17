using System.Linq;

namespace DataFlowPipeline
{
    using System;
    using System.IO;
    using System.IO.Compression;
    using System.Security.Cryptography;
    using System.Threading.Tasks;


    public static class IOUtils
    {
        public static async Task<ChunkBytes> Compress(ChunkBytes data)
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                using (GZipStream gzipStream = new GZipStream(memStream, CompressionLevel.Optimal, leaveOpen: true))
                    await gzipStream.WriteAsync(data.Bytes, 0, data.Length);
                var chunkBytes = new ChunkBytes(memStream.ToArray());
                return chunkBytes;
            }
        }

        public static async Task<ChunkBytes> Decompress(ChunkBytes data, int offset = 0)
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                using (MemoryStream input = new MemoryStream(data.Bytes, offset, data.Length - offset))
                using (GZipStream gzipStream = new GZipStream(input, CompressionMode.Decompress))
                    await gzipStream.CopyToAsync(memStream);

                var chunkBytes = new ChunkBytes(memStream.ToArray());
                return chunkBytes;
            }
        }

        public static string MD5FromBytes(ChunkBytes data)
        {
            using (var md5 = MD5.Create())
            using (var stream = new MemoryStream(data.Bytes, 0, data.Length))
                return BitConverter.ToString(md5.ComputeHash(stream)).Replace("-", "").ToLower();
        }

        private static readonly byte[] SALT =
            new byte[] {0x26, 0xdc, 0xff, 0x00, 0xad, 0xed, 0x7a, 0xee, 0xc5, 0xfe, 0x07, 0xaf, 0x4d, 0x08, 0x22, 0x3c};

        private static Lazy<Rijndael> RijndaelAlgorithm = new Lazy<Rijndael>(() =>
        {
            Rijndael rijndael = Rijndael.Create();
            Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes("buggghinabella", SALT);
            rijndael.Key = pdb.GetBytes(32);
            rijndael.IV = pdb.GetBytes(16);
            return rijndael;
        });

        public static Task<ChunkBytes> Encrypt(ChunkBytes data) =>
            CryptoTransform(data, RijndaelAlgorithm.Value.CreateEncryptor());

        public static Task<ChunkBytes> Decrypt(ChunkBytes data) =>
            CryptoTransform(data, RijndaelAlgorithm.Value.CreateDecryptor());

        private static async Task<ChunkBytes> CryptoTransform(ChunkBytes data, ICryptoTransform transform)
        {
            using (var memStream = new MemoryStream())
            {
                using (var cryptoStream = new CryptoStream(memStream, transform, CryptoStreamMode.Write))
                {
                    await cryptoStream.WriteAsync(data.Bytes, 0, data.Length);
                    cryptoStream.FlushFinalBlock();
                    var chunkBytes = new ChunkBytes(memStream.ToArray());
                    return chunkBytes;
                }
            }
        }

        public static ChunkBytes CombineByteArrays(params ChunkBytes[] args)
        {
            var buffer = new byte[1048576];

            int offSet = 0;
            for (int i = 0; i < args.Length; i++)
            {
                Buffer.BlockCopy(args[i].Bytes, 0, buffer, offSet, args[i].Length);
                offSet += args[i].Length;
            }

            var subArray = new byte[offSet];

            Array.Copy(buffer, 0, subArray, 0, offSet);

            return new ChunkBytes(subArray);
        }


        public static async Task<ChunkBytes> ReadFromStream(Stream streamSource, int chunkSize)
        {
            var buffer = new byte[chunkSize];
            var bytesRead = await streamSource.ReadAsync(buffer, 0, chunkSize);
            var subArray = new byte[bytesRead];
            Array.Copy(buffer, 0, subArray, 0, bytesRead);
            return new ChunkBytes(subArray);
        }
    }
}