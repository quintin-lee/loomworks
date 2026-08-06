# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Full project rename from `ctpool` to `loomworks`
- `docs/faq.md` — frequently asked questions
- `docs/migration.md` — migration guide from ctpool
- FAQ section added to README.md
- Expanded Notes section in README.md with signal handler and scheduler details
- Complete usage example in docs/api-reference.md
- Future work section in docs/design-decisions.md

### Changed
- All API prefixes: `ctpool_` -> `loom_`, `CTPPOOL_` -> `LOOMWORKS_`
- Include directory: `include/ctpool/` -> `include/loomworks/`
- Main header: `ctpool.h` -> `loomworks.h`
- CMake project name: `ctpool` -> `loomworks`
- Library output: `libctpool.a` -> `libloomworks.a`

### Fixed
- Fixed misleading blocks syntax (`^()`) in README examples; replaced with standard C function pointers

---

## [1.0.0]

### Added
- Thread pool with configurable worker count, stack size, and bounded/unbounded queue
- Future-based async result retrieval (`loom_pool_submit_future` + `loom_future_wait`)
- Stackful coroutine subsystem with mmap-allocated stacks
- PROT_NONE guard pages for stack overflow detection
- SIGSEGV/SIGBUS signal handler for safe stack overflow recovery
- Per-thread scheduler context (`_Thread_local`) for cross-thread safety
- Cache-line aligned structures to prevent false sharing
- Comprehensive test suite: 1025 pool assertions, 36 coroutine assertions, 50511 integration assertions
- Full API documentation in `docs/api-reference.md`
- Architecture documentation in `docs/architecture.md`
- Design decisions documentation in `docs/design-decisions.md`
- Contributing guide in `docs/contributing.md`

### Features
- Pure C11 implementation (no C++ dependencies)
- Graceful shutdown with task draining
- Opaque pointer API — internal struct definitions hidden from users
- 64-bit safe `makecontext` argument passing
- POSIX.1-2008 compliant (`_POSIX_C_SOURCE 200809L`)

### Build
- CMake build system with static and shared library targets
- Test integration via CTest
- gcc and clang compatible with `-Wall -Wextra -Werror -pedantic`

[Unreleased]: https://github.com/.../loomworks/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/.../loomworks/releases/tag/v1.0.0
