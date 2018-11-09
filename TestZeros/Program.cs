using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;

namespace TestZeros
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var cs = new CancellationTokenSource();
            Console.WriteLine("Searching non zeros ...");
            Console.WriteLine("Press enter to exit");
            try
            {
                var testTask = Test(cs.Token);
                var readLineTask = Task.Run(() =>
                {
                    Console.ReadLine();
                    cs.Cancel();
                });
                long nonZeroValue = await testTask;
                Console.WriteLine($"Found {nonZeroValue} in the file");
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Non zero values couldn't be found");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Something went wrong: \n" + ex.ToString());
            }
        }

        static unsafe Task<long> Test(CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                while (true)
                {
                    const int OneGigabyte = 1024 * 1024 * 1024;
                    string filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "mmap.bin");

                    // create mmap file and accessor, then adquire pointer 
                    var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
                    fileStream.SetLength(OneGigabyte);

                    var mmap = MemoryMappedFile.CreateFromFile(fileStream, null, fileStream.Length,
                        MemoryMappedFileAccess.ReadWrite,
                        null, HandleInheritability.None, true);

                    var accessor = mmap.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
                    byte* baseAddress = null;
                    accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref baseAddress);
                    byte* endAddress = baseAddress + OneGigabyte;
                    try
                    {
                        long* pointer = (long*)baseAddress;
                        while (pointer < endAddress)
                        {
                            if (cancellationToken.IsCancellationRequested) throw new OperationCanceledException();
                            if (*pointer != 0) return *pointer;
                            pointer++;
                        }
                        pointer = (long*)baseAddress;
                        while (pointer < endAddress)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            *pointer = -1L;
                            pointer++;
                        }
                        accessor.Flush();
                    }
                    finally
                    {
                        accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                        accessor.SafeMemoryMappedViewHandle.Close();
                        accessor.Dispose();
                        mmap.SafeMemoryMappedFileHandle.Close();
                        mmap.Dispose();
                        fileStream.Dispose();
                    }
                }
            });
        }
    }
}
