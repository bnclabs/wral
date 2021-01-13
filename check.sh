#! /usr/bin/env bash

# cargo +stable build // we are still using unstable features
cargo +nightly build

# cargo +stable test // we are still using unstable features.
cargo +nightly test

# cargo +stable bench // we are still using unstable features.
cargo +nightly bench

# cargo +stable doc // we are still using unstable features.
cargo +nightly doc

cargo +nightly clippy --all-targets --all-features

