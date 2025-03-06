using System;
using System.IO;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class FileBackedOutputStream : Stream
    {
        private readonly int _maxInMemoryBytes;
        private readonly string _tempDirForFiles;
        private MemoryStream _memoryOutputStream;
        private string _fileName;
        private FileStream _fileOutputStream;
        private Stream _outputStream;

        public FileBackedOutputStream(int maxInMemoryBytes, string tempDirForFiles)
        {
            _maxInMemoryBytes = maxInMemoryBytes;
            _tempDirForFiles = tempDirForFiles;
            _memoryOutputStream = new MemoryStream();
            _fileOutputStream = null;
            _outputStream = _memoryOutputStream;
        }

        public override void Flush()
        {
            _outputStream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _outputStream.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _outputStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _outputStream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            SwitchToFileIfTooMuchInMemory(count);
            _outputStream.Write(buffer, offset, count);
        }

        public override bool CanRead => _outputStream.CanRead;
        public override bool CanSeek => _outputStream.CanSeek;
        public override bool CanWrite => _outputStream.CanWrite;
        public override long Length => _outputStream.Length;
        public override long Position
        {
            get => _outputStream.Position;
            set => _outputStream.Position = value;
        }

        protected override void Dispose(bool disposing)
        {
            _outputStream.Close();
            base.Dispose(disposing);
        }

        internal String GetFileName()
        {
            return _fileName;
        }

        private void SwitchToFileIfTooMuchInMemory(int numberOfBytesToConsume)
        {
            if (IsUsingFileOutputStream())
            {
                return;
            }
            if (Length + numberOfBytesToConsume <= _maxInMemoryBytes)
            {
                return;
            }
            SwitchFromMemoryToTempFile();
        }

        internal bool IsUsingFileOutputStream()
        {
            return _fileOutputStream != null;
        }

        private void SwitchFromMemoryToTempFile()
        {
            _fileName = GenerateTempFilePath();
            _fileOutputStream = new FileStream(_fileName, FileMode.Create, FileAccess.ReadWrite, FileShare.Delete, 4096,
                FileOptions.DeleteOnClose);
            _memoryOutputStream.Position = 0;
            _memoryOutputStream.CopyTo(_fileOutputStream);
            _outputStream = _fileOutputStream;
            _memoryOutputStream.Close();
            _memoryOutputStream = null;
        }

        private string GenerateTempFilePath()
        {
            var millisFromEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var randomPart = Path.GetRandomFileName();
            var randomFileName = $"FileBackedOutputStream_{millisFromEpoch}_{randomPart}";
            return Path.Combine(_tempDirForFiles, randomFileName);
        }
    }
}
