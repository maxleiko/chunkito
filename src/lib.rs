use std::fs::File;
use std::path::Path;

use anyhow::{Context, Error};
use memmap2::{Mmap, MmapOptions};
use rayon::iter::plumbing::{bridge, Producer};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
    IntoParallelRefMutIterator, ParallelIterator,
};

pub struct Chunkit {
    mmap: Mmap,
    chunks: ChunkIter,
}

impl Chunkit {
    pub fn open<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path).context("unable to open file")?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let chunks = chunk_file(&mmap).context("unable to chunk the file")?;

        Ok(Self { mmap, chunks })
    }

    pub fn par_iter(&self) -> ParChunkIter<'_> {
        self.chunks.par_iter()
    }

    pub fn par_iter_mut(&mut self) -> ParChunkIterMut<'_> {
        self.chunks.par_iter_mut()
    }

    pub fn data(&self, chunk: Chunk) -> &str {
        let data = unsafe { std::str::from_utf8_unchecked(&self.mmap) };
        &data[chunk.start..chunk.end]
    }
}

pub struct ChunkIter {
    chunks: Vec<Chunk>,
    it: usize,
}

impl Iterator for ChunkIter {
    type Item = Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        if self.it < self.chunks.len() {
            let chunk = self.chunks[self.it];
            self.it += 1;
            Some(chunk)
        } else {
            None
        }
    }
}

pub struct ParChunkIter<'a> {
    chunks: &'a [Chunk],
}

pub struct ParChunkIterMut<'a> {
    chunks: &'a mut ChunkIter,
}

impl<'a> ParallelIterator for ParChunkIter<'a> {
    type Item = &'a Chunk;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(<Self as IndexedParallelIterator>::len(self))
    }
}

impl<'a> IndexedParallelIterator for ParChunkIter<'a> {
    fn len(&self) -> usize {
        self.chunks.len()
    }

    fn drive<C: rayon::iter::plumbing::Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB: rayon::iter::plumbing::ProducerCallback<Self::Item>>(
        self,
        callback: CB,
    ) -> CB::Output {
        callback.callback(ChunkProducer {
            chunks: self.chunks,
        })
    }
}

struct ChunkProducer<'a> {
    chunks: &'a [Chunk],
}

struct ChunkProducerMut<'a> {
    chunks: &'a mut [Chunk],
}

impl<'a> From<&'a mut [Chunk]> for ChunkProducerMut<'a> {
    fn from(chunks: &'a mut [Chunk]) -> Self {
        Self { chunks }
    }
}

impl<'a> From<ParChunkIterMut<'a>> for ChunkProducerMut<'a> {
    fn from(iterator: ParChunkIterMut<'a>) -> Self {
        Self {
            chunks: &mut iterator.chunks.chunks,
        }
    }
}

impl<'a> Producer for ChunkProducer<'a> {
    type Item = &'a Chunk;
    type IntoIter = ::std::slice::Iter<'a, Chunk>;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.iter()
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.chunks.split_at(index);
        (
            ChunkProducer { chunks: left },
            ChunkProducer { chunks: right },
        )
    }
}

impl<'a> Producer for ChunkProducerMut<'a> {
    type Item = &'a mut Chunk;
    type IntoIter = std::slice::IterMut<'a, Chunk>;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.iter_mut()
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.chunks.split_at_mut(index);
        (Self::from(left), Self::from(right))
    }
}

impl<'a> From<ParChunkIter<'a>> for ChunkProducer<'a> {
    fn from(value: ParChunkIter<'a>) -> Self {
        Self {
            chunks: value.chunks,
        }
    }
}

impl<'a> IntoParallelIterator for &'a ChunkIter {
    type Iter = ParChunkIter<'a>;
    type Item = &'a Chunk;

    fn into_par_iter(self) -> Self::Iter {
        ParChunkIter {
            chunks: &self.chunks,
        }
    }
}

impl<'a> IntoParallelIterator for &'a mut ChunkIter {
    type Iter = ParChunkIterMut<'a>;
    type Item = &'a mut Chunk;

    fn into_par_iter(self) -> Self::Iter {
        ParChunkIterMut { chunks: self }
    }
}

impl<'a> ParallelIterator for ParChunkIterMut<'a> {
    type Item = &'a mut Chunk;
    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(<Self as IndexedParallelIterator>::len(self))
    }
}

impl<'a> IndexedParallelIterator for ParChunkIterMut<'a> {
    fn with_producer<CB: rayon::iter::plumbing::ProducerCallback<Self::Item>>(
        self,
        callback: CB,
    ) -> CB::Output {
        let producer = ChunkProducerMut::from(self);
        callback.callback(producer)
    }

    fn drive<C: rayon::iter::plumbing::Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn len(&self) -> usize {
        self.chunks.chunks.len()
    }
}

/// A chunk contains lines without overlapping
#[derive(Clone, Copy, Debug)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
}

// TODO make sure we are not in an escaped LF when trying to find('\n')
// cause right now that's a bug
fn chunk_file(mmap: &Mmap) -> Result<ChunkIter, Error> {
    let data = unsafe { std::str::from_utf8_unchecked(mmap) };

    let eof = data.len();

    // split the file based on the number of thread
    let nb_chunks = num_cpus::get();
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

    Ok(ChunkIter { chunks, it: 0 })
}
