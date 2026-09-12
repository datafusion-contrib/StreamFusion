use lzokay::compress::compress_worst_size;
use std::io::{self, Read, Write};

pub(super) const BLOCK_SIZE: usize = 64 * 1024;

/// Task-local framing: uncompressed length, compressed length (both u32 LE), then LZO bytes.
/// Bounded blocks let Arrow IPC stream through the codec without buffering the whole spill.
pub(super) type Compressor = Box<dyn FnMut(&[u8], &mut [u8]) -> io::Result<usize>>;

pub(super) struct Encoder<W, C> {
    output: W,
    compressor: C,
    compressed: Vec<u8>,
}

impl<W: Write, C> Encoder<W, C> {
    pub(super) fn new(output: W, compressor: C) -> Self {
        Self {
            output,
            compressor,
            compressed: vec![0; compress_worst_size(BLOCK_SIZE)],
        }
    }
}

impl<W: Write, C: FnMut(&[u8], &mut [u8]) -> io::Result<usize>> Write for Encoder<W, C> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }
        let input = &input[..input.len().min(BLOCK_SIZE)];
        let size = (self.compressor)(input, &mut self.compressed)?;
        if size == 0 || size > compress_worst_size(input.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid LZO compressor output length",
            ));
        }
        self.output.write_all(&(input.len() as u32).to_le_bytes())?;
        self.output.write_all(&(size as u32).to_le_bytes())?;
        self.output.write_all(&self.compressed[..size])?;
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

pub(super) fn java_compressor(
    env: &mut jni::JNIEnv,
    codec: jni::objects::JObject,
) -> jni::errors::Result<Compressor> {
    let vm = env.get_java_vm()?;
    let codec = env.new_global_ref(codec)?;
    Ok(Box::new(move |input, output| {
        let mut env = vm.get_env().map_err(io::Error::other)?;
        env.with_local_frame(2, |env| -> jni::errors::Result<usize> {
            // The Java callback borrows these buffers synchronously and retains neither address.
            let source =
                unsafe { env.new_direct_byte_buffer(input.as_ptr() as *mut u8, input.len())? };
            let target = unsafe { env.new_direct_byte_buffer(output.as_mut_ptr(), output.len())? };
            Ok(env
                .call_method(
                    codec.as_obj(),
                    "compress",
                    "(Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I",
                    &[
                        jni::objects::JValue::Object(source.as_ref()),
                        jni::objects::JValue::Object(target.as_ref()),
                    ],
                )?
                .i()? as usize)
        })
        .map_err(io::Error::other)
    }))
}

pub(super) struct Decoder<R> {
    input: R,
    compressed: Vec<u8>,
    decoded: Vec<u8>,
    position: usize,
}

impl<R: Read> Decoder<R> {
    pub(super) fn new(input: R) -> Self {
        Self {
            input,
            compressed: Vec::new(),
            decoded: Vec::new(),
            position: 0,
        }
    }
}

impl<R: Read> Read for Decoder<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.position == self.decoded.len() {
            let mut header = [0; 8];
            if self.input.read(&mut header[..1])? == 0 {
                return Ok(0);
            }
            self.input.read_exact(&mut header[1..])?;
            let decoded_size = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
            let compressed_size = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;
            if !(1..=BLOCK_SIZE).contains(&decoded_size)
                || !(1..=compress_worst_size(decoded_size)).contains(&compressed_size)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid LZO spill block size",
                ));
            }
            self.compressed.resize(compressed_size, 0);
            self.input.read_exact(&mut self.compressed)?;
            self.decoded.resize(decoded_size, 0);
            let size = lzokay::decompress::decompress(&self.compressed, &mut self.decoded)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if size != decoded_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "LZO spill block length mismatch",
                ));
            }
            self.position = 0;
        }
        let count = output.len().min(self.decoded.len() - self.position);
        output[..count].copy_from_slice(&self.decoded[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lzokay::compress::{compress_no_alloc, Dict};
    use std::io::BufWriter;

    fn encode(input: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut dictionary = Dict::new();
        let compressor = |input: &[u8], output: &mut [u8]| {
            compress_no_alloc(input, output, &mut dictionary).map_err(io::Error::other)
        };
        let mut encoder =
            BufWriter::with_capacity(BLOCK_SIZE, Encoder::new(&mut bytes, compressor));
        for chunk in input.chunks(79) {
            encoder.write_all(chunk).unwrap();
        }
        encoder.flush().unwrap();
        drop(encoder);
        bytes
    }

    #[test]
    fn round_trips_empty_partial_and_multiple_blocks_with_bounded_buffers() {
        for size in [0, 1, BLOCK_SIZE - 1, BLOCK_SIZE, BLOCK_SIZE * 3 + 17] {
            let mut random = 1_u64;
            let input: Vec<u8> = (0..size)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    random as u8
                })
                .collect();
            let bytes = encode(&input);
            let mut decoder = Decoder::new(bytes.as_slice());
            let mut actual = Vec::new();
            let mut chunk = [0; 113];
            loop {
                assert_eq!(decoder.read(&mut []).unwrap(), 0);
                let size = decoder.read(&mut chunk).unwrap();
                if size == 0 {
                    break;
                }
                actual.extend_from_slice(&chunk[..size]);
                assert!(decoder.decoded.len() <= BLOCK_SIZE);
                assert!(decoder.compressed.len() <= compress_worst_size(BLOCK_SIZE));
            }
            assert_eq!(actual, input);
        }
    }

    #[test]
    fn rejects_truncated_corrupt_and_oversized_blocks() {
        let bytes = encode(&vec![42; BLOCK_SIZE]);
        let read = |bytes: &[u8]| Decoder::new(bytes).read_to_end(&mut Vec::new());
        for end in [1, 7, 8, bytes.len() - 1] {
            assert!(read(&bytes[..end]).is_err());
        }
        for (offset, value) in [
            (0, 0),
            (0, BLOCK_SIZE as u32 + 1),
            (4, u32::MAX),
            (0, BLOCK_SIZE as u32 - 1),
        ] {
            let mut corrupt = bytes.clone();
            corrupt[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            assert!(read(&corrupt).is_err());
        }
        let mut corrupt = bytes;
        corrupt[8..].fill(0);
        assert!(read(&corrupt).is_err());
        let mut corrupt = encode(&[42; 100]);
        corrupt[..4].copy_from_slice(&101_u32.to_le_bytes());
        assert!(read(&corrupt).is_err());
    }
}
