using System.Reactive.Concurrency;

namespace DataFlowPipeline
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    public class CompressAndEncryptPipeline
    {
        public CompressAndEncryptPipeline(int maxDegreeOfParallelism, int chunkSize = 1048576,
            int boundedCapacity = 20)
        {
            _chunkSize = chunkSize;
            _boundedCapacity = boundedCapacity;
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
        }

        private readonly int _chunkSize;
        private readonly int _boundedCapacity;
        private readonly int _maxDegreeOfParallelism;

        public async Task CompressAndEncrypt(
            Stream streamSource, Stream streamDestination,
            CancellationTokenSource cts = null)
        {
            cts = cts ?? new CancellationTokenSource();

            var compressorOptions = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                BoundedCapacity = _boundedCapacity,
                CancellationToken = cts.Token
            };

            var inputBuffer = new BufferBlock<CompressingDetails>(
                new DataflowBlockOptions
                {
                    CancellationToken = cts.Token,
                    BoundedCapacity = _boundedCapacity
                });

            var compressor = new TransformBlock<CompressingDetails, CompressedDetails>(
                async details =>
                {
                    ChunkBytes compressedData = await IOUtils.Compress(details.Bytes);

                    return new CompressedDetails
                    {
                        Bytes = compressedData,
                        ChunkSize = details.ChunkSize,
                        Sequence = details.Sequence,
                        CompressedDataSize = new ChunkBytes(BitConverter.GetBytes(compressedData.Length))
                    };
                }, compressorOptions);

            var encryptor = new TransformBlock<CompressedDetails, EncryptDetails>(
                async details =>
                {
                    var data = IOUtils.CombineByteArrays(details.CompressedDataSize, details.ChunkSize,
                        details.Bytes);
                    var encryptedData = await IOUtils.Encrypt(data);

                    return new EncryptDetails
                    {
                        Bytes = encryptedData,
                        Sequence = details.Sequence,
                        EncryptedDataSize = new ChunkBytes(BitConverter.GetBytes(encryptedData.Length))
                    };
                }, compressorOptions);

            // Enables Rx integration with TPL Dataflow 
            encryptor.AsObservable()
                .Scan((new Dictionary<int, EncryptDetails>(), 0),
                    (state, msg) =>
                        // Runs Rx Scan operation asynchronously
                        Observable.FromAsync(async () =>
                            {
                                (Dictionary<int, EncryptDetails> details, int lastIndexProc) = state;
                                details.Add(msg.Sequence, msg);
                                while (details.ContainsKey(lastIndexProc + 1))
                                {
                                    msg = details[lastIndexProc + 1];

                                    // Persists asynchronously the data; the stream could be replaced 
                                    // with a network stream to send the data across the wire.
                                    await streamDestination.WriteAsync(msg.EncryptedDataSize.Bytes, 0,
                                        msg.EncryptedDataSize.Length);
                                    await streamDestination.WriteAsync(msg.Bytes.Bytes, 0, msg.Bytes.Length);
                                    lastIndexProc = msg.Sequence;

                                    // the chunk of data that is processed is removed 
                                    // from the local state, keeping track of the items to perform
                                    details.Remove(lastIndexProc);
                                }

                                return (details, lastIndexProc);
                            })
                            .SingleAsync().Wait())
                // Rx subscribes to TaskPoolScheduler
                .SubscribeOn(TaskPoolScheduler.Default).Subscribe();

            var linkOptions = new DataflowLinkOptions {PropagateCompletion = true};
            inputBuffer.LinkTo(compressor, linkOptions);
            compressor.LinkTo(encryptor, linkOptions);

            long sourceLength = streamSource.Length;
            byte[] size = BitConverter.GetBytes(sourceLength);
            await streamDestination.WriteAsync(size, 0, size.Length);


            var chunkSize = (_chunkSize < sourceLength) ? _chunkSize : (int) sourceLength;
            int indexSequence = 0;
            while (sourceLength > 0)
            {
                var bytes = await IOUtils.ReadFromStream(streamSource, chunkSize);
                var compressingDetails = new CompressingDetails
                {
                    Bytes = bytes,
                    ChunkSize = new ChunkBytes(BitConverter.GetBytes(bytes.Length)),
                    Sequence = ++indexSequence
                };
                await inputBuffer.SendAsync(compressingDetails);
                sourceLength -= bytes.Length;
                if (sourceLength < chunkSize)
                    chunkSize = (int) sourceLength;
                if (sourceLength == 0)
                    inputBuffer.Complete();
            }

            await inputBuffer.Completion.ContinueWith(task => compressor.Complete());
            await compressor.Completion.ContinueWith(task => encryptor.Complete());
            await encryptor.Completion;
            await streamDestination.FlushAsync();
        }
        
        
        public async Task DecryptAndDecompress(
                                    Stream streamSource, Stream streamDestination,
                                    CancellationTokenSource cts = null)
        {
            cts = cts ?? new CancellationTokenSource();

            var compressorOptions = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                CancellationToken = cts.Token,
                BoundedCapacity = _boundedCapacity
            };

            var inputBuffer = new BufferBlock<DecryptDetails>(
                                    new DataflowBlockOptions
                                    {
                                        CancellationToken = cts.Token,
                                        BoundedCapacity = _boundedCapacity
                                    });

            var decryptor = new TransformBlock<DecryptDetails, DecompressionDetails>(
                async details =>
                {
                    var decryptedData = await IOUtils.Decrypt(details.Bytes);
                  
                    return new DecompressionDetails
                    {
                        Bytes = decryptedData,
                        Sequence = details.Sequence,
                    };
                }, compressorOptions);

            var decompressor = new TransformBlock<DecompressionDetails, DecompressionDetails>(
                async details =>
            {
                var decompressedData = await IOUtils.Decompress(details.Bytes, sizeof(int) + sizeof(int));
                
                return new DecompressionDetails
                {
                    Bytes = decompressedData,
                    Sequence = details.Sequence
                };
            }, compressorOptions);

            
            decompressor.AsObservable()
                .Scan((new Dictionary<int, DecompressionDetails>(), 0),
                    (state, msg) =>
                        // Runs Rx Scan operation asynchronously
                        Observable.FromAsync(async () =>
                            {
                                (Dictionary<int, DecompressionDetails> details, int lastIndexProc) = state;
                                details.Add(msg.Sequence, msg);
                                while (details.ContainsKey(lastIndexProc + 1))
                                {
                                    msg = details[lastIndexProc + 1];
                                    await streamDestination.WriteAsync(msg.Bytes.Bytes, 0, msg.Bytes.Length);
                                    lastIndexProc = msg.Sequence;
                                    details.Remove(lastIndexProc);
                                }

                                return (details, lastIndexProc);
                            })
                            .SingleAsync().Wait())
                // Rx subscribes to TaskPoolScheduler
                .SubscribeOn(TaskPoolScheduler.Default).Subscribe();

           
            var linkOptions = new DataflowLinkOptions() { PropagateCompletion = true };
            inputBuffer.LinkTo(decryptor, linkOptions);
            decryptor.LinkTo(decompressor, linkOptions);
            
            GCSettings.LargeObjectHeapCompactionMode =
                GCLargeObjectHeapCompactionMode.CompactOnce;

            byte[] size = new byte[sizeof(long)];
            await streamSource.ReadAsync(size, 0, size.Length);
            // convert the size to number
            long destinationLength = BitConverter.ToInt64(size, 0);
            streamDestination.SetLength(destinationLength);
            long sourceLength = streamSource.Length - sizeof(long);

            int index = 0;
            while (sourceLength > 0)
            {
                // read the encrypted chunk size
                size = new byte[sizeof(int)];
                int sizeReadCount = await streamSource.ReadAsync(size, 0, size.Length);

                // convert the size back to number
                int storedSize = BitConverter.ToInt32(size, 0);

                var encryptedData = await IOUtils.ReadFromStream(streamSource, storedSize);

                var decryptDetails = new DecryptDetails
                {
                    Bytes = encryptedData,
                    EncryptedDataSize = new ChunkBytes(size),
                    Sequence = ++index
                };

                await inputBuffer.SendAsync(decryptDetails);

                sourceLength -= (sizeReadCount + encryptedData.Length);
                if (sourceLength == 0)
                    inputBuffer.Complete();
            }
            
            await inputBuffer.Completion.ContinueWith(task => decryptor.Complete());
            await decryptor.Completion.ContinueWith(task => decompressor.Complete());
            await decompressor.Completion;

            await streamDestination.FlushAsync();
        }
    }
}