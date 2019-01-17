using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataFlowPipeline
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await RunGcComparison(new[] {1, 2, 3});
            Console.ReadLine();
        }

        private static string workDirectory = @"./Data";
        private static string templateFile = @"./Data/TextData.txt";

        public static async Task CompressAndEncryptTest(string srcFile, string dstFile, string rstFile)
        {
            try
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();

                Console.WriteLine("CompressAndEncrypt ...");
                var dataflow = new CompressAndEncryptPipeline(Environment.ProcessorCount);

                if (File.Exists(dstFile))
                    File.Delete(dstFile);

                using (var streamSource = new FileStream(srcFile, FileMode.OpenOrCreate, FileAccess.Read,
                    FileShare.None, 0x1000, useAsync: true))
                using (var streamDestination = new FileStream(dstFile, FileMode.OpenOrCreate, FileAccess.Write,
                    FileShare.None, 0x1000, useAsync: true))
                {
                    await Benchaark("CompressAndEncrypt", async () =>
                    {
                        await dataflow.CompressAndEncrypt(streamSource, streamDestination);
                        streamDestination.Close();
                    });
                }

                Console.WriteLine("Press Enter to continue");
                Console.ReadLine();
                sw.Restart();

                if (File.Exists(rstFile))
                    File.Delete(rstFile);

                Console.WriteLine("DecryptAndDecompress ...");
                using (var streamSource = new FileStream(dstFile, FileMode.OpenOrCreate, FileAccess.Read,
                    FileShare.None, 0x1000, useAsync: true))
                using (var streamDestination = new FileStream(rstFile, FileMode.OpenOrCreate, FileAccess.Write,
                    FileShare.None, 0x1000, useAsync: true))
                {
                    await Benchaark("DecryptAndDecompress",
                        async () => { await dataflow.DecryptAndDecompress(streamSource, streamDestination); });
                }

                Console.WriteLine("Press Enter to continue");
                Console.ReadLine();

                Console.WriteLine("Verification ...");
                using (var f1 = File.OpenRead(srcFile))
                using (var f2 = File.OpenRead(rstFile))
                {
                    bool ok = false;
                    if (f1.Length == f2.Length)
                    {
                        ok = true;
                        int count;
                        const int size = 0x1000000;

                        var buffer = new byte[size];
                        var buffer2 = new byte[size];

                        while ((count = f1.Read(buffer, 0, buffer.Length)) > 0 && ok == true)
                        {
                            f2.Read(buffer2, 0, count);
                            ok = buffer2.SequenceEqual(buffer);
                            if (!ok) break;
                        }
                    }

                    Console.WriteLine($"Restored file isCorrect = {ok}");
                }

                Console.WriteLine("Press Enter to continue");
                Console.ReadLine();
            }
            catch (AggregateException ex)
            {
                var q = new Queue<Exception>(new[] {ex});
                while (q.Count > 0)
                {
                    var e = q.Dequeue();
                    Console.WriteLine($"\t{e.Message}");
                    if (e is AggregateException)
                    {
                        foreach (var x in (e as AggregateException).InnerExceptions)
                            q.Enqueue(x);
                    }
                    else
                    {
                        if (e.InnerException != null)
                            q.Enqueue(e.InnerException);
                    }
                }
            }
        }

        private static void CreateTextFileWithSize(string path, string templateFilePath, long targetSize)
        {
            var bytes = File.ReadAllBytes(templateFilePath);

            using (FileStream fs = new FileStream(path, FileMode.Append, FileAccess.Write))
            {
                var iterations = (targetSize - fs.Length + bytes.Length) / bytes.Length;

                for (var i = 0; i < iterations; i++)
                    fs.Write(bytes, 0, bytes.Length);
            }
        }

        private static async Task RunGcComparison(int[] fileSizesInGb)
        {
            const long bytesInGb = 1024L * 1024 * 1024;
            string inFile = Path.Combine(workDirectory, "inFile.txt");
            string outFile = Path.Combine(workDirectory, "outFile.txt");


            foreach (var size in fileSizesInGb)
            {
                Console.WriteLine($"Creating input file {size}GB ...");
                if (File.Exists(inFile))
                    File.Delete(inFile);
                CreateTextFileWithSize(inFile, templateFile, bytesInGb * size);

                if (File.Exists(outFile))
                    File.Delete(outFile);
                Console.WriteLine("GC.Collect() ...");
                GC.Collect();
                GC.Collect();
                Console.WriteLine($"Running compression test...");

                await CompressAndEncryptTest(inFile, outFile,
                    Path.Combine(workDirectory, Path.GetFileName(inFile) + "_copy" + Path.GetExtension(inFile)));
            }
        }

        private static async Task Benchaark(string label, Func<Task> operation)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();

            // do a full GC at the start but NOT thereafter
            // allow garbage to collect for each iteration
            System.GC.Collect();
            Console.WriteLine("Started");


            Func<Tuple<int, int, int, long>> getGcStats = () =>
            {
                var gen0 = System.GC.CollectionCount(0);
                var gen1 = System.GC.CollectionCount(1);
                var gen2 = System.GC.CollectionCount(2);
                var mem = System.GC.GetTotalMemory(false);
                return Tuple.Create(gen0, gen1, gen2, mem);
            };

            Console.WriteLine("=======================");
            Console.WriteLine(label);
            Console.WriteLine("=======================");

            var gcStatsBegin = getGcStats();
            stopwatch.Restart();
            await operation();
            stopwatch.Stop();
            var gcStatsEnd = getGcStats();
            var changeInMem = (gcStatsEnd.Item4 - gcStatsBegin.Item4) / 1000L;
            Console.WriteLine(
                $"Operation {label} elapsed:{stopwatch.ElapsedMilliseconds}ms gen0:{(gcStatsEnd.Item1 - gcStatsBegin.Item1)} gen1:{(gcStatsEnd.Item2 - gcStatsBegin.Item2)} gen2:{(gcStatsBegin.Item3 - gcStatsEnd.Item3)} mem:{changeInMem}");
        }
    }
}