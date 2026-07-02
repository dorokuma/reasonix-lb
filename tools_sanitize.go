package main

import (
	"log"
	"encoding/json"
	"sync"
)

// namespaceToolMap tracks the mapping from prefixed tool names (used in Chat Completions)
// back to the original namespace + sub-tool name (used in Responses API).
// Key: prefixed name (e.g. "mcp__tavily__tavily_search")
// Value: original sub-tool name (e.g. "tavily_search")
var (
	namespaceToolMap      = map[string]string{}
	namespacePrefixMap    = map[string]string{}
	namespaceToolMapMu    sync.RWMutex
	namespaceNameMap      = map[string]string{}
	namespaceNameMapMu    sync.RWMutex
)

func registerNamespaceTool(prefixed, original string) {
	namespaceToolMapMu.Lock()
	defer namespaceToolMapMu.Unlock()
	namespaceToolMap[prefixed] = original
	namespacePrefixMap[original] = prefixed
}

func registerNamespaceName(prefixed, bundleName string) {
	namespaceNameMapMu.Lock()
	defer namespaceNameMapMu.Unlock()
	namespaceNameMap[prefixed] = bundleName
}

// NamespaceForTool returns the namespace bundle name for a given prefixed tool name.
// Pass the original prefixed name (e.g. "mcp__codegraph__files") returned by the upstream model.
// Returns empty string if the tool is not part of a namespace bundle.
func NamespaceForTool(prefixedName string) string {
	namespaceNameMapMu.RLock()
	defer namespaceNameMapMu.RUnlock()
	return namespaceNameMap[prefixedName]
}

// ResolveNamespaceTool looks up a prefixed tool name and returns the original sub-tool name.
// If no mapping exists, returns the input unchanged.
func ResolveNamespaceTool(name string) string {
	namespaceToolMapMu.RLock()
	defer namespaceToolMapMu.RUnlock()
	if orig, ok := namespaceToolMap[name]; ok {
		return orig
	}
	return name
}

// PrefixNamespaceTool maps an original sub-tool name to its prefixed form.
// Returns the input unchanged if no mapping exists.
func PrefixNamespaceTool(name string) string {
	namespaceToolMapMu.RLock()
	defer namespaceToolMapMu.RUnlock()
	if prefixed, ok := namespacePrefixMap[name]; ok {
		return prefixed
	}
	return name
}

// sanitizeToolsForChatCompletions converts Responses API tools to Chat Completions format.
// Filters out Codex-internal types that have no Chat Completions equivalent.
// Namespace bundles are flattened with prefixed names to preserve routing info.
func sanitizeToolsForChatCompletions(raw json.RawMessage) any {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return jsonRawToAny(raw)
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, flattenToolEntry(item)...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func flattenToolEntry(item json.RawMessage) []map[string]any {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(item, &m); err != nil {
		return nil
	}
	typ, _ := rawStringField(m, "type")
	switch typ {
	case "code_interpreter", "file_search", "computer_use":
		log.Printf("tools_sanitize: dropping unsupported tool type %q", typ)
		return nil
	}
	if typ == "web_search" {
		params := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string", "description": "Search query"},
			},
			"required": []string{"query"},
		}
		if extra := jsonRawToAny(m["parameters"]); extra != nil {
			if em, ok := extra.(map[string]any); ok {
				if props, ok := em["properties"].(map[string]any); ok {
					for k, v := range props {
						if k != "query" {
							params["properties"].(map[string]any)[k] = v
						}
					}
				}
			}
		}
		return []map[string]any{{
			"type": "function",
			"function": map[string]any{
				"name":        "web_search",
				"description": "Search the web for current information on any topic.",
				"parameters":  params,
			},
		}}
	}
	// Namespace / MCP bundle: { name, description, tools: [...] }
	if nested, ok := m["tools"]; ok && len(nested) > 0 && string(nested) != "null" {
		bundleName, _ := rawStringField(m, "name")
		var sub []json.RawMessage
		if err := json.Unmarshal(nested, &sub); err != nil {
			return nil
		}
		var out []map[string]any
		for _, s := range sub {
			var sm map[string]json.RawMessage
			if err := json.Unmarshal(s, &sm); err != nil {
				continue
			}
			subName, _ := rawStringField(sm, "name")
			if subName == "" {
				if fnRaw, ok := sm["function"]; ok {
					var fn map[string]json.RawMessage
					if json.Unmarshal(fnRaw, &fn) == nil {
						subName, _ = rawStringField(fn, "name")
					}
				}
			}
			if t := asFunctionTool(sm); t != nil {
				// Prefix with bundle name to preserve namespace routing
				if bundleName != "" && subName != "" {
					prefixed := bundleName + "__" + subName
					registerNamespaceTool(prefixed, subName)
					registerNamespaceName(prefixed, bundleName)
					if fnObj, ok := t["function"].(map[string]any); ok {
						fnObj["name"] = prefixed
					}
				}
				out = append(out, t)
			}
		}
		return out
	}
	// tool_search: deferred MCP tool discovery (Codex v0.142.5+)
	if typ == "tool_search" {
		desc, _ := rawStringField(m, "description")
		fnObj := map[string]any{"name": "tool_search"}
		if desc != "" {
			fnObj["description"] = desc
		}
		if len(m["parameters"]) > 0 && string(m["parameters"]) != "null" {
			fnObj["parameters"] = simplifyJSONSchema(jsonRawToAny(m["parameters"]))
		} else {
			fnObj["parameters"] = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		return []map[string]any{{
			"type":     "function",
			"function": fnObj,
		}}
	}
	if t := asFunctionTool(m); t != nil {
		return []map[string]any{t}
	}
	return nil
}

func asFunctionTool(m map[string]json.RawMessage) map[string]any {
	var name, desc string
	var params json.RawMessage

	if fnRaw, ok := m["function"]; ok && len(fnRaw) > 0 && string(fnRaw) != "null" {
		var fn map[string]json.RawMessage
		if err := json.Unmarshal(fnRaw, &fn); err == nil {
			name, _ = rawStringField(fn, "name")
			desc, _ = rawStringField(fn, "description")
			params = fn["parameters"]
		}
	}
	if name == "" {
		var ok bool
		name, ok = rawStringField(m, "name")
		if !ok || name == "" {
			return nil
		}
	}
	if desc == "" {
		desc, _ = rawStringField(m, "description")
	}
	if len(params) == 0 || string(params) == "null" {
		params = m["parameters"]
	}

	fnObj := map[string]any{"name": name}
	if desc != "" {
		fnObj["description"] = desc
	}
	if len(params) > 0 && string(params) != "null" {
		fnObj["parameters"] = simplifyJSONSchema(jsonRawToAny(params))
	} else {
		fnObj["parameters"] = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return map[string]any{
		"type":     "function",
		"function": fnObj,
	}
}
