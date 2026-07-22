use std::io::{self, Read};

use crate::transport::webrtc::video::{
    annexb::AnnexBSplitter,
    h264::{Nal, NalHeader},
};

pub struct H264Reader<R: Read> {
    annex_b: AnnexBSplitter<R>,
}

impl<R> H264Reader<R>
where
    R: Read,
{
    pub fn new(reader: R, capacity: usize) -> Self {
        Self {
            annex_b: AnnexBSplitter::new(reader, capacity),
        }
    }

    pub fn next_nal(&mut self) -> Result<Option<Nal>, io::Error> {
        if let Some(annex_b) = self.annex_b.next()? {
            if annex_b.payload_range.len() < NalHeader::SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "h264 NAL is shorter than its header",
                ));
            }

            let header_range =
                annex_b.payload_range.start..(annex_b.payload_range.start + NalHeader::SIZE);

            let mut header = [0u8; 1];
            header.copy_from_slice(&annex_b.full[header_range.clone()]);
            let header = NalHeader::parse(header);

            let payload_range = header_range.end..annex_b.payload_range.end;

            Ok(Some(Nal {
                payload_range,
                header,
                header_range,
                start_code: annex_b.start_code,
                start_code_range: annex_b.start_code_range,
                full: annex_b.full,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn reset(&mut self, new_reader: R) {
        self.annex_b.reset(new_reader);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn short_nal_returns_invalid_data() {
        let mut reader = H264Reader::new(Cursor::new(vec![0, 0, 1]), 8);
        let error = reader.next_nal().err().expect("empty NAL must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
