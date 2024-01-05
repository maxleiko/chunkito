use std::{fs::File, result::Result};

use anyhow::Context;
use chunkito::Chunkit;
use fxhash::FxHashMap;
use rayon::prelude::*;

fn main() -> anyhow::Result<()> {
    let chunks = Chunkit::open("../measurements.txt").context("unable to chunk file")?;

    chunks.par_iter().enumerate().for_each(|(index, chunk)| {
        let data = chunks.data(*chunk);
        let mut sensors: FxHashMap<&str, Sensor> = fxhash::FxHashMap::default();

        data.lines()
            .map(|line| line.split_once(';').unwrap())
            .map(|(name, temp)| (name, temp.parse::<f32>().unwrap()))
            .for_each(|(name, temp)| {
                sensors
                    .entry(name)
                    .and_modify(|sensor| sensor.add_temp(temp))
                    .or_insert_with(|| Sensor {
                        cnt: 1,
                        min: temp,
                        max: temp,
                        sum: temp as f64,
                    });
            });

        let output = File::create(format!("chunk-{index}.json"))
            .expect("unable to create chunk result file");
        serde_json::to_writer(output, &sensors).expect("unable to serialize result to JSON");
    });

    Ok(())
}

struct Sensor {
    min: f32,
    sum: f64,
    cnt: usize,
    max: f32,
}

impl Sensor {
    fn add_temp(&mut self, temp: f32) {
        if temp < self.min {
            self.min = temp;
        }
        if temp > self.max {
            self.max = temp;
        }
        self.sum += temp as f64;
        self.cnt += 1;
    }
}

impl Default for Sensor {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            sum: 0.0,
            cnt: 0,
            max: f32::MIN,
        }
    }
}

impl serde::Serialize for Sensor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Sensor", 4)?;
        s.serialize_field("min", &self.min)?;
        s.serialize_field("avg", &(self.sum / self.cnt as f64))?;
        s.serialize_field("max", &self.max)?;
        s.serialize_field("count", &self.cnt)?;
        s.end()
    }
}
