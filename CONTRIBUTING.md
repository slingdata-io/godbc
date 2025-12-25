# Contributing to godbc

Thank you for your interest in contributing to godbc! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- **Go**: Version 1.24 or later
- **ODBC Driver Manager**:
  - **Windows**: Built-in (odbc32.dll)
  - **macOS**: `brew install unixodbc`
  - **Linux**: `apt install unixodbc-dev` or `yum install unixODBC-devel`
- **Database ODBC Drivers**: Install drivers for databases you want to test against

### Clone

```bash
git clone https://github.com/slingdata-io/godbc.git
cd godbc
```

### Running Tests

```bash
# Run unit tests
go test -v ./...

# Run benchmarks
go test -bench=. -benchmem ./...
```

### Integration Testing

Build and run the test example against a database:

```bash
go build ./examples/basic/

# SQL Server
./basic -conn-string "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=test;UID=sa;PWD=password;Encrypt=no"

# PostgreSQL
./basic -conn-string "Driver={PostgreSQL Unicode};Server=localhost;Database=test;UID=postgres;PWD=password" -schema public

# MySQL
./basic -conn-string "Driver={MySQL ODBC 8.0 Unicode Driver};Server=localhost;Database=test;UID=root;PWD=password"

# SQLite
./basic -conn-string "Driver={SQLite3 ODBC Driver};Database=/tmp/test.db"
```

## Code Style

- Run `gofmt` on all Go files before committing
- Follow standard Go naming conventions
- Add godoc comments to all exported types and functions
- Keep functions focused and reasonably sized

```bash
# Format code
gofmt -w .

# Check for issues
go vet ./...
```

## Pull Request Process

1. **Fork** the repository and create a feature branch
2. **Write tests** for new functionality
3. **Update documentation** if adding new features
4. **Run tests** to ensure nothing is broken
5. **Submit a PR** with a clear description of changes

### PR Guidelines

- Keep PRs focused on a single change
- Reference any related issues
- Include test coverage for new code
- Update README.md if adding user-facing features

## Reporting Issues

When reporting bugs, please include:

- Go version (`go version`)
- Operating system and architecture
- ODBC driver name and version
- Database type and version
- Minimal code to reproduce the issue
- Full error messages and stack traces

## Feature Requests

For feature requests, please describe:

- The problem you're trying to solve
- Your proposed solution
- Any alternatives you've considered

## Code of Conduct

Be respectful and constructive in all interactions. We welcome contributors of all experience levels.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
