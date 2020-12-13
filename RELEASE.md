Release Checklist
=================

* Check and verify serialization `ID` for Cborized types.
* Documentation Review.
* Bump up the version:
  * __major__: backward incompatible API changes.
  * __minor__: backward compatible API Changes.
  * __patch__: bug fixes.
* README
  * Link to rust-doc.
  * Short description.
  * Contribution guidelines.
* Cargo checklist
  * cargo +stable build; cargo +nightly build
  * cargo +stable doc; cargo +nightly doc
  * cargo +stable test; cargo +nightly test
  * cargo +nightly bench
  * cargo +nightly clippy --all-targets --all-features
* Cargo spell check.
* Create a git-tag for the new version.
* Cargo publish the new version.

(optional)

* Travis-CI integration.
* Badges
  * Build passing, Travis continuous integration.
  * Code coverage, codecov and coveralls.
  * Crates badge
  * Downloads badge
  * License badge
  * Rust version badge.
  * Maintenance-related badges based on isitmaintained.com
  * Documentation
  * Gitpitch

