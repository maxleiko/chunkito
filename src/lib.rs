use std::{fs::File, path::Path};

use anyhow::{Context, Error};
use memmap2::MmapOptions;

/// A chunk contains lines without overlapping
#[derive(Clone, Copy, Debug)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
}

// TODO make sure we are not in an escaped LF when trying to find('\n')
// cause right now that's a bug
pub fn chunk_it<P: AsRef<Path>>(path: P, nb_chunks: usize) -> Result<Vec<Chunk>, Error> {
    let file = File::open(path).context("unable to open file")?;
    let mmap = unsafe {
        MmapOptions::new()
            .map_copy_read_only(&file)
            .context("unable to mmap the file")?
    };
    let data = unsafe { std::str::from_utf8_unchecked(&mmap) };
    let eof = data.len();

    // split the file based on the number of thread
    let mut chunks = Vec::with_capacity(nb_chunks);
    let chunk_size = eof / nb_chunks;

    let mut offset = 0;
    for _ in 0..nb_chunks {
        let mut end = offset + chunk_size;
        // if after eof then, eof
        if end >= eof {
            end = eof;
        }
        if offset == end {
            break;
        }
        // else, try to find the closest LF char
        match data[end..].find('\n') {
            Some(lf) => {
                end += lf + 1;
                chunks.push(Chunk { start: offset, end });
                offset = end;
            }
            None => {
                chunks.push(Chunk {
                    start: offset,
                    end: eof,
                });
                break;
            }
        }
    }

    Ok(chunks)
}
