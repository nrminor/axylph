//! Independent pinned sylph CLI oracle shared by integration tests.
//!
//! `default.json` is a lossless YAML-to-JSON conversion of the sylph fork's pinned
//! `default.yaml` (SHA-256 8af09a9fbabe124676d25ae9d00a24e0e59bde756b28d16ab506b37617fef966).
//! The reference database is copied byte-for-byte. Neither oracle was regenerated.

#![cfg(test)]

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
pub struct Fixture {
    pub(super) revision: String,
    pub(super) c: usize,
    pub(super) k: usize,
    pub(super) no_dedup: bool,
    pub(super) reads: Vec<String>,
    pub(super) prefixes: Vec<Prefix>,
}

#[derive(Deserialize)]
pub struct Prefix {
    pub(super) end: usize,
    pub(super) rows: Vec<BTreeMap<String, String>>,
}

#[track_caller]
fn wire_number(value: &Value) -> f64 {
    let number = value
        .as_f64()
        .expect("wire scientific value must be a JSON number");
    assert!(number.is_finite(), "wire scientific value must be finite");
    number
}

#[track_caller]
pub fn assert_wire_profile(profile: &Value, expected: &Prefix) {
    assert_eq!(profile["sample_name"], "synthetic-sample");
    assert_eq!(
        profile["options"],
        json!({
            "minimum_ani_percent": 95.0, "min_contain": 7, "min_number_kmers": 10.0,
            "confidence_intervals": true, "mode": "relative_abundance", "read_identity_percent": null
        })
    );
    let rows = profile["rows"]
        .as_array()
        .expect("profile rows must be an array");
    assert_eq!(rows.len(), expected.rows.len());
    for expected in &expected.rows {
        let row = rows
            .iter()
            .find(|row| {
                row["genome_name"] == expected["Genome_file"]
                    && row["contig_name"] == expected["Contig_name"]
            })
            .expect("wire profile must contain every independently pinned row");
        // The independent CLI oracle pins precision, not every unrounded digit.
        for (name, actual, exponent) in [
            (
                "Adjusted_ANI",
                wire_number(&row["adjusted_ani_fraction"]).min(1.0) * 100.0,
                -2,
            ),
            (
                "Naive_ANI",
                wire_number(&row["naive_ani_fraction"]) * 100.0,
                -2,
            ),
            ("Eff_cov", wire_number(&row["effective_coverage"]), -3),
            ("Mean_cov_geq1", wire_number(&row["mean_coverage"]), -3),
            (
                "Taxonomic_abundance",
                wire_number(&row["taxonomic_abundance_percent"]),
                -4,
            ),
        ] {
            let pinned: f64 = expected[name].parse().unwrap();
            let tolerance = 0.5_f64.mul_add(10_f64.powi(exponent), 1e-12);
            assert!(
                (actual - pinned).abs() <= tolerance,
                "{name}: actual {actual}, pinned {pinned}"
            );
        }
        assert_eq!(
            wire_number(&row["median_coverage"]),
            expected["Median_cov"].parse::<f64>().unwrap()
        );
        let (contained, sampled) = expected["Containment_ind"].split_once('/').unwrap();
        assert_eq!(
            row["containment"],
            json!({"contained": contained, "sampled": sampled})
        );
        assert_eq!(row["kmers_reassigned"], expected["kmers_reassigned"]);
        assert_eq!(row["adjustment"]["kind"], "lambda");
        assert_eq!(
            format!("{:.3}", wire_number(&row["adjustment"]["value"])),
            expected["Eff_lambda"]
        );
        assert_eq!(row["sequence_estimate"]["kind"], "percent");
        assert_eq!(
            format!("{:.4}", wire_number(&row["sequence_estimate"]["value"])),
            expected["Sequence_abundance"]
        );
        for (field, name, scale) in [
            ("ani_interval_fraction", "ANI_5-95_percentile", 100.0),
            ("lambda_interval", "Lambda_5-95_percentile", 1.0),
        ] {
            let interval = row[field]
                .as_array()
                .expect("prefix-23 oracle has confidence intervals");
            assert_eq!(interval.len(), 2);
            assert_eq!(
                format!(
                    "{:.2}-{:.2}",
                    wire_number(&interval[0]) * scale,
                    wire_number(&interval[1]) * scale
                ),
                expected[name]
            );
        }
    }
}
