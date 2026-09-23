use memchr::memmem::Finder;
use std::cmp;
use std::io::Error;
use tokio_util::bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

#[derive(Clone)]
pub struct MemMemDelimiterCodec<'a> {
    finder: Finder<'a>,
    delim_size: usize,
    is_discarding: bool,
    next_index: usize,
    max_length: usize,
}

#[derive(Debug)]
pub enum MemMemDelimiterCodecError {
    MaxChunkLengthExceeded,
    Io(Error),
}

impl From<Error> for MemMemDelimiterCodecError {
    fn from(e: Error) -> Self {
        MemMemDelimiterCodecError::Io(e)
    }
}

impl<'a> MemMemDelimiterCodec<'a> {
    pub fn new<T: ?Sized + AsRef<[u8]>>(delimiter: &'a T) -> Self {
        let finder = Finder::new(delimiter);
        let delim_size = finder.needle().iter().len();

        MemMemDelimiterCodec {
            finder,
            delim_size,
            is_discarding: false,
            next_index: 0,
            max_length: usize::MAX,
        }
    }

    pub fn new_with_max_length<T: ?Sized + AsRef<[u8]>>(
        delimiter: &'a T,
        max_length: usize,
    ) -> Self {
        MemMemDelimiterCodec {
            max_length,
            ..MemMemDelimiterCodec::new(delimiter)
        }
    }
}

impl Decoder for MemMemDelimiterCodec<'_> {
    type Item = Bytes;
    type Error = MemMemDelimiterCodecError;

    // implementation details shamelessly stolen from AnyDelimiterCodec
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let read_to = cmp::min(self.max_length.saturating_add(self.delim_size), buf.len());
            let slice = &buf[self.next_index..read_to];

            let new_chunk_offset = self.finder.find(slice);

            match (self.is_discarding, new_chunk_offset) {
                (true, Some(offset)) => {
                    // some delimiter found, but we were discarding
                    // + DELIM_SIZE => chop off with delimiter
                    buf.advance(offset + self.next_index + self.delim_size);
                    self.is_discarding = false;
                    self.next_index = 0; // rewind to start as incriminated section was chopped
                    // no return, continue reading buffer in loop
                }
                (true, None) => {
                    // discarding and we didn't find delimiter
                    // no delimiter found till end of slice
                    buf.advance(read_to); // chop off
                    // we continue discarding (self.is_discarding still true)
                    self.next_index = 0;

                    if buf.is_empty() {
                        return Ok(None); // waiter! more bytes please 😋️
                    }
                }
                (false, Some(offset)) => {
                    // not discarding and we found some delimiter
                    let new_chunk_index = offset + self.next_index;
                    self.next_index = 0;
                    // + DELIM_SIZE => message will contain delimiter
                    let chunk = buf.split_to(new_chunk_index + self.delim_size);

                    return Ok(Some(chunk.freeze()));
                }
                // no delimiter found and reached max length
                (false, None) if buf.len() > self.max_length => {
                    // return error (max length reached) and start discarding on next call
                    self.is_discarding = true;

                    return Err(MemMemDelimiterCodecError::MaxChunkLengthExceeded);
                }
                (false, None) => {
                    // no delimiter found but didn't reach length limit
                    self.next_index = read_to; // skip it!

                    return Ok(None); // waiter... I am still hungry 🥺️
                }
            }
        }
    }

    // from AnyDelimiterCodec
    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(match self.decode(buf)? {
            Some(frame) => Some(frame),
            None => {
                // return remaining data, if any
                if buf.is_empty() {
                    None
                } else {
                    let chunk = buf.split_to(buf.len());
                    self.next_index = 0;
                    Some(chunk.freeze())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemMemDelimiterCodec;
    use std::assert_matches;
    use std::cmp::Ordering;
    use std::io::{Error, ErrorKind};
    use tokio_stream::StreamExt;
    use tokio_test::io::Builder;
    use tokio_util::codec::FramedRead;

    #[tokio::test]
    async fn test_chunks() {
        let messages = Builder::new()
            .read(
                b"\
        DoubleDelimiterCodec\nDoubleDelimiterCodec\n\n\
        DoubleDelimiterCodec2\nDoubleDelimiterCodec2\n\n",
            )
            .build();
        let first_message = b"DoubleDelimiterCodec\nDoubleDelimiterCodec\n\n";
        let second_message = b"DoubleDelimiterCodec2\nDoubleDelimiterCodec2\n\n";

        let mut reader = FramedRead::new(messages, MemMemDelimiterCodec::new(b"\n\n"));

        let bytes = reader.next().await.unwrap().unwrap();
        let result = bytes.as_ref();
        debug_assert_eq!(result.cmp(first_message), Ordering::Equal);

        let bytes = reader.next().await.unwrap().unwrap();
        let result = bytes.as_ref();
        debug_assert_eq!(result.cmp(second_message), Ordering::Equal);
    }

    #[tokio::test]
    async fn test_io_error_signalled() {
        let ioe = Builder::new()
            .read(b"aslkjdlk\n\n")
            .read_error(Error::new(ErrorKind::BrokenPipe, "connection closed"))
            .build();

        let mut reader = FramedRead::new(ioe, MemMemDelimiterCodec::new(b"\n\n"));

        reader.next().await.unwrap().unwrap();
        assert_matches!(
            reader.next().await,
            Some(Err(MemMemDelimiterCodecError::Io(_)))
        );
    }

    #[tokio::test]
    async fn test_remaining_bytes_consumed() {
        let ioe = Builder::new().read(b"abc\ndef\nhij").build();
        let message1 = b"abc\n";
        let message2 = b"def\n";
        let rest = b"hij";

        let mut reader = FramedRead::new(ioe, MemMemDelimiterCodec::new(b"\n"));

        assert_eq!(
            reader.next().await.unwrap().unwrap().as_ref().cmp(message1),
            Ordering::Equal
        );
        assert_eq!(
            reader.next().await.unwrap().unwrap().as_ref().cmp(message2),
            Ordering::Equal
        );
        assert_eq!(
            reader.next().await.unwrap().unwrap().as_ref().cmp(rest),
            Ordering::Equal
        );

        assert_matches!(reader.next().await, None);
    }
}
