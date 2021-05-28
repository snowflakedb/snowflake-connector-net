﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    // Optimized for maximum speed when adding one byte at a time to short buffers
    public class FastMemoryStream
    {
        byte[] _buffer;
        int _size;

        public FastMemoryStream()
        {
            _buffer = new byte[256];
            _size = 0;
        }

        public void WriteByte(byte b)
        {
            if (_size == _buffer.Length)
                GrowBuffer();
            _buffer[_size] = b;
            _size++;
        }

        public void Clear()
        {
            // We reuse the same buffer, we also do not bother to clear the buffer
            _size = 0;
        }

        public byte[] GetBuffer()
        {
            // Note that we return a reference to the actual buffer. No copying here
            return _buffer;
        }

        public int Length => _size;

        private void GrowBuffer()
        {
            // Create a new array with double the size and copy existing elements to the new array
            byte[] newBuffer = new byte[_buffer.Length * 2];
            Array.Copy(_buffer, newBuffer, _size);
            _buffer = newBuffer;
        }
    }
}
