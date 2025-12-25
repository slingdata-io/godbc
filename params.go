package godbc

// ParameterError represents an error with parameter binding
type ParameterError struct {
	Name    string
	Message string
}

func (e *ParameterError) Error() string {
	if e.Name != "" {
		return "parameter '" + e.Name + "': " + e.Message
	}
	return "parameter: " + e.Message
}

// NamedParams holds parsed named parameter information
type NamedParams struct {
	// Query is the converted query with positional ? placeholders
	Query string

	// Names contains the parameter names in order of their first appearance
	Names []string

	// Positions maps parameter names to their positions (1-based, matching ODBC binding)
	// A single named parameter may appear multiple times in the query
	Positions map[string][]int
}

// ParseNamedParams parses a query with named parameters and converts to positional placeholders.
// Supports the following named parameter styles:
//   - :name  (Oracle/PostgreSQL style)
//   - @name  (SQL Server style)
//   - $name  (PostgreSQL style - not $1 which is positional)
//
// Returns nil if no named parameters are found (query uses positional ? only).
// The original query is preserved if it contains only ? placeholders.
func ParseNamedParams(query string) *NamedParams {
	if len(query) == 0 {
		return nil
	}

	// Quick scan to see if we have any named parameters
	hasNamed := false
	for i := 0; i < len(query); i++ {
		c := query[i]
		if c == ':' || c == '@' || c == '$' {
			// Check if followed by a valid identifier start
			if i+1 < len(query) && isIdentStart(query[i+1]) {
				hasNamed = true
				break
			}
		}
	}

	if !hasNamed {
		return nil
	}

	result := &NamedParams{
		Positions: make(map[string][]int),
	}

	var output []byte
	position := 0
	i := 0

	for i < len(query) {
		c := query[i]

		// Skip string literals (single quotes)
		if c == '\'' {
			start := i
			i++
			for i < len(query) {
				if query[i] == '\'' {
					if i+1 < len(query) && query[i+1] == '\'' {
						// Escaped quote
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			output = append(output, query[start:i]...)
			continue
		}

		// Skip string literals (double quotes - identifiers)
		if c == '"' {
			start := i
			i++
			for i < len(query) {
				if query[i] == '"' {
					if i+1 < len(query) && query[i+1] == '"' {
						// Escaped quote
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			output = append(output, query[start:i]...)
			continue
		}

		// Skip comments (-- style)
		if c == '-' && i+1 < len(query) && query[i+1] == '-' {
			start := i
			for i < len(query) && query[i] != '\n' {
				i++
			}
			output = append(output, query[start:i]...)
			continue
		}

		// Skip comments (/* */ style)
		if c == '/' && i+1 < len(query) && query[i+1] == '*' {
			start := i
			i += 2
			for i+1 < len(query) {
				if query[i] == '*' && query[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
			output = append(output, query[start:i]...)
			continue
		}

		// Check for named parameter
		if (c == ':' || c == '@' || c == '$') && i+1 < len(query) && isIdentStart(query[i+1]) {
			// Extract the parameter name
			start := i + 1
			end := start
			for end < len(query) && isIdentChar(query[end]) {
				end++
			}

			name := query[start:end]
			position++

			// Record the position for this name
			result.Positions[name] = append(result.Positions[name], position)

			// Add to names list if first occurrence
			found := false
			for _, n := range result.Names {
				if n == name {
					found = true
					break
				}
			}
			if !found {
				result.Names = append(result.Names, name)
			}

			// Replace with ?
			output = append(output, '?')
			i = end
			continue
		}

		// Regular character - copy as-is
		output = append(output, c)
		i++
	}

	if len(result.Names) == 0 {
		return nil
	}

	result.Query = string(output)
	return result
}

// isIdentStart returns true if c is a valid identifier start character
func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

// isIdentChar returns true if c is a valid identifier character
func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}
