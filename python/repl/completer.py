"""
repl/completer.py

ReplCompleter: namespace-aware, depends only on UISpec + CompletionCache.
No grpc, no proto, no I/O.
"""

from __future__ import annotations

import shlex
from typing import Callable, Iterator, Optional

from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document
from prompt_toolkit.formatted_text import FormattedText

from google.protobuf.descriptor_pb2 import FieldDescriptorProto

from repl.schema.ui_spec import (
    CandidateRow,
    CompletionCache,
    CompletionSource,
    CmdNode,
    FieldSpec,
    UISpec,
)


# ──────────────────────────────────────────────────────────────────────────────
# Meta-commands — built-ins that always appear in Tab completion.
# Tuples of (name, description, only_when_in_namespace).
# ──────────────────────────────────────────────────────────────────────────────
_META_COMMANDS: list[tuple[str, str, bool]] = [
    ("use", "Enter a namespace  (e.g. use key)", False),
    ("help", "List available commands", False),
    ("exit", "Quit the REPL", False),
    ("back", "Return to parent namespace", True),
    ("..", "Return to parent namespace", True),
    ("services", "List gRPC services (reflection)", False),
]


class ReplCompleter(Completer):
    """
    Completion logic for the main prompt.

    Namespace awareness
    ───────────────────
    namespace_fn() returns the active namespace tuple, e.g. ("key",) after
    `use key`.  The completer uses it in two ways:

    1. Command-path completion (empty buffer or partial word):
       - Strips the active prefix from candidate paths and shows only the
         *next segment*, so `use key` + Tab shows  get / put / watch  not
         key.get / key.put / key.watch.
       - Absolute paths that don't start with the active prefix are still
         shown so the user can always escape the namespace.

    2. Node resolution (for flag/value completion):
       - If `words[0]` isn't an absolute command, tries namespace + words[0].
    """

    def __init__(
        self,
        ui_spec: UISpec,
        cache: CompletionCache,
        namespace_fn: Callable[[], tuple[str, ...]] = lambda: (),
    ) -> None:
        self._spec = ui_spec
        self._cache = cache
        self._namespace_fn = namespace_fn
        self._last_completion_source: Optional[CompletionSource] = None

    # ── Public ───────────────────────────────────────────────────────────────

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        text = document.text_before_cursor
        after_space = text.endswith(" ")

        try:
            words = shlex.split(text)
        except ValueError:
            words = text.split()

        # ── Top-level / partial command name ─────────────────────────────────
        if not words or (len(words) == 1 and not after_space):
            yield from self._complete_cmd_path(words[0] if words else "")
            return

        # ── Resolve node (namespace-aware) ────────────────────────────────────
        node = self._resolve_node(words[0])
        if node is None:
            if not after_space:
                yield from self._complete_cmd_path(words[0])
            return

        last = words[-1]

        # Case A: "cmd --flag <TAB>" — complete the value
        if after_space and last.startswith("--"):
            yield from self._complete_value(node, last.lstrip("-"), "")
            return

        # Case B: "cmd --flag partial<TAB>" — complete partial value
        if not after_space and not last.startswith("-") and len(words) >= 3:
            prev = words[-2]
            if prev.startswith("--"):
                yield from self._complete_value(node, prev.lstrip("-"), last)
                return

        # Case C: "cmd --partial<TAB>" — complete flag name, preserve dashes
        if not after_space and last.startswith("-"):
            raw_partial = last
            flag_partial = last.lstrip("-")
            yield from self._complete_flags(node, words, raw_partial, flag_partial)
            return

        # Case D: "cmd <TAB>" after a completed value, or just after cmd
        yield from self._complete_flags(node, words, "", "")

    def current_completion_source(
        self, document: Document
    ) -> Optional[CompletionSource]:
        return self._last_completion_source

    # ── Namespace helpers ─────────────────────────────────────────────────────

    def _active_prefix(self) -> str:
        ns = self._namespace_fn()
        return ".".join(ns) if ns else ""

    def _resolve_node(self, word: str) -> Optional[CmdNode]:
        """Find the CmdNode for word, trying absolute then namespace-prefixed."""
        node = self._spec.find_cmd(word)
        if node:
            return node
        prefix = self._active_prefix()
        if prefix:
            node = self._spec.find_cmd(f"{prefix}.{word}")
        return node

    # ── Command-path completion ───────────────────────────────────────────────

    def _complete_cmd_path(self, partial: str) -> Iterator[Completion]:
        """
        Emit command completions, relativized to the active namespace.

        With no namespace:
          ""    → top-level segments: "key" (3 commands), "services"
          "key" → namespace entry "key"
          "key."→ leaves: "key.get", "key.put", …

        With namespace ("key",):
          ""    → next segments relative to "key.": get, put, watch
          "g"   → filtered: get
          If partial contains "." or starts an absolute path not under
          the namespace, fall back to absolute resolution so the user can
          always escape.
        """
        prefix = self._active_prefix()
        seen: set[str] = set()

        if prefix and not partial:
            # ── Relative mode: show next segments under active namespace ──────
            ns_dot = prefix + "."
            for cmd, node in self._spec.commands.items():
                if node.bootstrap or not cmd.startswith(ns_dot):
                    continue
                remainder = cmd[len(ns_dot) :]  # e.g. "get" or "search.regex"
                next_seg = remainder.split(".")[0]  # e.g. "get" or "search"
                if next_seg in seen:
                    continue
                seen.add(next_seg)

                # Is this a namespace (multiple commands) or a leaf?
                sub = [
                    c
                    for k, c in self._spec.commands.items()
                    if k.startswith(ns_dot + next_seg) and not c.bootstrap
                ]
                if len(sub) > 1:
                    meta = f"{len(sub)} commands"
                else:
                    meta = node.description[:40] if node.description else ""

                yield Completion(
                    text=next_seg,
                    start_position=0,
                    display=FormattedText([("class:completion-cmd", next_seg)]),
                    display_meta=meta,
                )
            # Also show aliases that resolve into this namespace
            for alias, canonical in self._spec.alias_map.items():
                if alias in seen:
                    continue
                node = self._spec.find_cmd(canonical)
                if node and not node.bootstrap and canonical.startswith(ns_dot):
                    seen.add(alias)
                    yield Completion(
                        text=alias,
                        start_position=0,
                        display=FormattedText(
                            [
                                ("class:completion-alias", alias),
                                ("class:completion-sep", "  "),
                                (
                                    "class:completion-meta",
                                    f"→ {canonical[len(ns_dot) :]}",
                                ),
                            ]
                        ),
                    )
            # Meta-commands (back, .., use, help, exit) always visible
            yield from self._complete_meta("", seen)
            return

        # ── Absolute / partial mode (no namespace, or partial has content) ────
        show_full = "." in partial
        candidates: list[tuple[str, str, str]] = []

        for cmd, node in self._spec.commands.items():
            if node.bootstrap or not cmd.startswith(partial):
                continue

            if show_full:
                insert = cmd[len(partial) :]
                display = cmd
                meta = node.description[:40] if node.description else ""
            else:
                remainder = cmd[len(partial) :]
                first_seg = remainder.split(".")[0]
                insert = first_seg
                sub = self._spec.subtree(partial + first_seg)
                if len(sub) > 1:
                    display = partial + first_seg
                    meta = f"{len(sub)} commands"
                else:
                    display = cmd
                    meta = node.description[:40] if node.description else ""

            if insert not in seen:
                seen.add(insert)
                candidates.append((insert, display, meta))

        candidates.sort(key=lambda t: (0 if "." not in t[0] else 1, t[1]))
        for insert, display, meta in candidates:
            yield Completion(
                text=insert,
                start_position=0,
                display=FormattedText([("class:completion-cmd", display)]),
                display_meta=meta,
            )

        # Aliases
        for alias, canonical in self._spec.alias_map.items():
            if not alias.startswith(partial) or alias in seen:
                continue
            node = self._spec.find_cmd(canonical)
            if node is None or node.bootstrap:
                continue
            seen.add(alias)
            yield Completion(
                text=alias[len(partial) :],
                start_position=0,
                display=FormattedText(
                    [
                        ("class:completion-alias", alias),
                        ("class:completion-sep", "  "),
                        ("class:completion-meta", f"→ {canonical}"),
                    ]
                ),
                display_meta=node.description[:30] if node.description else "",
            )

        # Meta-commands (use, help, exit, back, …)
        yield from self._complete_meta(partial, seen)

    # ── Meta-command completion ──────────────────────────────────────────────

    def _complete_meta(self, partial: str, seen: set[str]) -> Iterator[Completion]:
        """
        Yield built-in meta-commands (use, help, exit, back, ..) filtered by
        `partial` and de-duped against `seen`.  Namespace-only commands (back,
        ..) are suppressed when at the root.
        """
        in_ns = bool(self._namespace_fn())
        for name, description, ns_only in _META_COMMANDS:
            if ns_only and not in_ns:
                continue
            if not name.startswith(partial):
                continue
            if name in seen:
                continue
            seen.add(name)
            yield Completion(
                text=name[len(partial) :],
                start_position=0,
                display=FormattedText([("class:completion-meta", name)]),
                display_meta=description,
            )

        # ── Flag name completion ──────────────────────────────────────────────────

    def _complete_flags(
        self,
        node: CmdNode,
        words: list[str],
        raw_partial: str,  # typed text including dashes, e.g. "--k"
        flag_partial: str,  # just the name part, e.g. "k"
    ) -> Iterator[Completion]:
        """
        Yield --flag completions.

        `text` is always the full "--name" string.
        `start_position` is negative by len(raw_partial) so the dashes already
        typed are replaced cleanly:

          buffer "cmd "    → start=0,  inserts "--key"   → "cmd --key"
          buffer "cmd --"  → start=-2, inserts "--key"   → "cmd --key"
          buffer "cmd --k" → start=-3, inserts "--key"   → "cmd --key"
        """
        used: set[str] = {
            w.lstrip("-") for w in words[1:] if w.startswith("--") and w != raw_partial
        }
        start = -len(raw_partial)

        for fld in node.input_fields:
            if fld.hidden or fld.name in used:
                continue
            if not fld.name.startswith(flag_partial):
                continue
            full = f"--{fld.name}"
            yield Completion(
                text=full,
                start_position=start,
                display=FormattedText([("class:completion-flag", full)]),
                display_meta=_type_label(fld),
            )

    # ── Flag value completion ─────────────────────────────────────────────────

    def _complete_value(
        self, node: CmdNode, field_name: str, partial: str
    ) -> Iterator[Completion]:
        field = next((f for f in node.input_fields if f.name == field_name), None)
        if field is None:
            self._last_completion_source = None
            return

        # Enum
        if field.enum_values:
            self._last_completion_source = None
            for v in field.enum_values:
                if v.startswith(partial):
                    yield Completion(
                        text=v[len(partial) :], start_position=0, display=v
                    )
            return

        # Bool
        if field.proto_type == FieldDescriptorProto.TYPE_BOOL:
            self._last_completion_source = None
            for v in ("true", "false"):
                if v.startswith(partial):
                    yield Completion(v[len(partial) :], start_position=0)
            return

        # RPC-backed
        comp_src = node.completion_for_field(field_name)
        if comp_src is None:
            self._last_completion_source = None
            return

        self._last_completion_source = comp_src
        for row in self._cache.get(comp_src.source_rpc):
            if not row.insert_value.startswith(partial):
                continue
            yield Completion(
                text=row.insert_value[len(partial) :],
                start_position=0,
                display=_render_candidate_row(row, comp_src.column_separator),
            )


# ──────────────────────────────────────────────────────────────────────────────
# Header-aware wrapper
# ──────────────────────────────────────────────────────────────────────────────


class HeaderAwareCompleter(Completer):
    def __init__(self, inner: ReplCompleter) -> None:
        self._inner = inner

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        completions = list(self._inner.get_completions(document, complete_event))

        if completions:
            src = self._inner.current_completion_source(document)
            if src and src.show_headers and src.display_fields:
                yield Completion(
                    text="",
                    start_position=0,
                    display=_render_header_row(src),
                    style="class:completion-header-row",
                )

        yield from completions


# ──────────────────────────────────────────────────────────────────────────────
# Rendering helpers
# ──────────────────────────────────────────────────────────────────────────────


def _render_candidate_row(row: CandidateRow, separator: str = "  ") -> FormattedText:
    parts: list[tuple[str, str]] = []
    for i, (text, style) in enumerate(row.display_cols):
        if i > 0:
            parts.append(("class:completion-sep", separator))
        parts.append((style or "class:completion-meta", text))
    if not parts:
        parts = [("", row.insert_value)]
    return FormattedText(parts)


def _render_header_row(source: CompletionSource) -> FormattedText:
    parts: list[tuple[str, str]] = []
    sep = source.column_separator or "  "
    for i, df in enumerate(source.display_fields):
        if i > 0:
            parts.append(("class:completion-header-sep", sep))
        label = df.display_label()
        w = df.width or len(label)
        parts.append(("class:completion-header", label.ljust(w)[:w]))
    return FormattedText(parts)


def _type_label(field: FieldSpec) -> str:
    from repl.schema.parser import _proto_type_label

    return _proto_type_label(field)
