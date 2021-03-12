# Package not ready for stable.

build:
	# ... build ...
	cargo +nightly build
	# ... test ...
	cargo +nightly test --no-run
	# ... bench ...
	cargo +nightly bench --no-run
	# ... doc ...
	cargo +nightly doc
	# ... bins ...
	cargo +nightly build --release --bin perf --features=perf
	# ... meta commands ...
	cargo +nightly clippy --all-targets --all-features
flamegraph:
	cargo flamegraph --features=perf --bin=perf -- --payload 100 --ops 10000 --threads 1 --size 100000000
prepare:
	check.sh
	perf.sh
clean:
	cargo clean
	rm -f check.out perf.out flamegraph.svg perf.data perf.data.old
