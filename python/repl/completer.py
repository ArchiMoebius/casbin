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
from repl.schema.projection import _render_display_cols


# ──────────────────────────────────────────────────────────────────────────────
# Meta-commands — built-ins that always appear at the right level.
# Tuples of (name, description, only_when_in_namespace).
# ──────────────────────────────────────────────────────────────────────────────
_META_COMMANDS: list[tuple[str, str, bool]] = [
    ("use", "Enter a namespace  (e.g. use key)", False),
    ("help", "List available commands", False),
    ("exit", "Quit the REPL", False),
    ("back", "Return to parent namespace", True),
    ("..", "Return to parent namespace", True),
]


class ReplCompleter(Completer):
    """
    Completion logic for the main prompt.

    Namespace awareness
    ───────────────────
    namespace_fn() returns the active namespace tuple, e.g. ("key",) after
    `use key`.

    Command-path completion:
      - Always operates in *relative mode* when a namespace is active:
        strips the prefix and shows only next segments.
      - Aliases are NEVER shown in the command-path dropdown (they work when
        typed directly but clutter the menu).

    live_fetch_fn:
      Optional callback the runtime provides. Called synchronously before
      reading from cache when a completion source has live:true.
    """

    def __init__(
        self,
        ui_spec: UISpec,
        cache: CompletionCache,
        namespace_fn: Callable[[], tuple[str, ...]] = lambda: (),
        live_fetch_fn: Optional[
            Callable[[CompletionSource, dict[str, str]], None]
        ] = None,
    ) -> None:
        self._spec = ui_spec
        self._cache = cache
        self._namespace_fn = namespace_fn
        self._live_fetch_fn = live_fetch_fn
        self._last_completion_source: Optional[CompletionSource] = None

    # ── Public ───────────────────────────────────────────────────────────────

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        # Reset header state on every invocation so it doesn't linger after
        # a completion is accepted.
        self._last_completion_source = None

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

        # ── `use <arg>` completion ────────────────────────────────────────────
        if words[0] == "use":
            partial = words[1] if len(words) == 2 and not after_space else ""
            yield from self._complete_use_arg(partial)
            return

        # ── `help <arg>` completion ───────────────────────────────────────────
        if words[0] in ("help", "?"):
            partial = words[1] if len(words) == 2 and not after_space else ""
            yield from self._complete_help_arg(partial)
            return

        # ── Resolve node (namespace-aware) ────────────────────────────────────
        node = self._resolve_node(words[0])
        if node is None:
            if not after_space:
                yield from self._complete_cmd_path(words[0])
            return

        last = words[-1]

        # Build a dict of already-typed flag values from the buffer.
        # Used by live completions to expand ${VAR} in source_request.
        # e.g. ["config.get", "--env", "dev", "--key"] → {"env": "dev", "ENV": "dev"}
        typed_flags = _extract_typed_flags(words)

        # Case A: "cmd --flag <TAB>" — complete the value
        if after_space and last.startswith("--"):
            yield from self._complete_value(node, last.lstrip("-"), "", typed_flags)
            return

        # Case B: "cmd --flag partial<TAB>" — complete partial value
        if not after_space and not last.startswith("-") and len(words) >= 3:
            prev = words[-2]
            if prev.startswith("--"):
                yield from self._complete_value(
                    node, prev.lstrip("-"), last, typed_flags
                )
                return

        # Case C: "cmd --partial<TAB>" — complete flag name
        if not after_space and last.startswith("-"):
            raw_partial = last
            flag_partial = last.lstrip("-")
            yield from self._complete_flags(node, words, raw_partial, flag_partial)
            return

        # Case D: "cmd <TAB>" — show remaining flags
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
        """
        Find the CmdNode for word.
        Namespace-prefixed tried first (mirrors model.resolve_cmd).
        """
        prefix = self._active_prefix()
        if prefix:
            node = self._spec.find_cmd(f"{prefix}.{word}")
            if node:
                return node
        # Absolute path or alias
        return self._spec.find_cmd(word)

    # ── Command-path completion ───────────────────────────────────────────────

    def _complete_cmd_path(self, partial: str) -> Iterator[Completion]:
        """
        Emit command completions scoped to the active namespace.

        Rules:
          • When namespace is active:  always relative mode.
            - partial=""  → next segments under ns prefix
            - partial="g" → segments under ns prefix that start with "g"
          • At root with partial="":   top-level segments only.
          • At root with partial="ke": segments matching "ke…"

        Aliases are NEVER emitted here. They resolve silently on Enter.
        """
        prefix = self._active_prefix()
        seen: set[str] = set()

        if prefix:
            # ── Relative mode: scoped to active namespace ──────────────────
            ns_dot = prefix + "."
            for cmd in sorted(self._spec.commands):
                node = self._spec.commands[cmd]
                if node.bootstrap or not cmd.startswith(ns_dot):
                    continue
                remainder = cmd[len(ns_dot) :]  # e.g. "get", "get.active"
                if not remainder.startswith(partial):
                    continue
                # Next segment after partial
                suffix = remainder[len(partial) :]  # e.g. "" / "et" / "t.active"
                next_seg = suffix.split(".")[0]  # e.g. "" / "et" / "t"
                full_seg = partial + next_seg  # e.g. "g"→"get"
                if not next_seg or full_seg in seen:
                    continue
                seen.add(full_seg)

                # Count sub-commands at this segment
                full_path = ns_dot + full_seg
                sub = [
                    k
                    for k in self._spec.commands
                    if k.startswith(full_path) and not self._spec.commands[k].bootstrap
                ]
                # If exactly 1 and its path == full_path, it's a leaf
                is_leaf = len(sub) == 1 and sub[0] == full_path
                if len(sub) > 1 or not is_leaf:
                    # namespace or leaf — determine display
                    has_children = any(
                        k.startswith(full_path + ".")
                        for k in self._spec.commands
                        if not self._spec.commands[k].bootstrap
                    )
                    if has_children and len(sub) > 1:
                        meta = f"{len(sub)} commands"
                    else:
                        meta = node.description[:40] if node.description else ""
                else:
                    meta = node.description[:40] if node.description else ""

                yield Completion(
                    text=next_seg,  # suffix to append to what's typed
                    start_position=0,
                    display=FormattedText([("class:completion-cmd", full_seg)]),
                    display_meta=meta,
                )
            yield from self._complete_meta(partial, seen)
            return

        # ── Root mode ────────────────────────────────────────────────────────
        # No aliases; only top-level segments (or their matching sub-segments
        # when partial contains a ".").
        show_full = "." in partial

        for cmd in sorted(self._spec.commands):
            node = self._spec.commands[cmd]
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
                yield Completion(
                    text=insert,
                    start_position=0,
                    display=FormattedText([("class:completion-cmd", display)]),
                    display_meta=meta,
                )

        # Meta-commands (use, help, exit) — NO aliases
        yield from self._complete_meta(partial, seen)

    # ── use <arg> completion ─────────────────────────────────────────────────

    def _complete_use_arg(self, partial: str) -> Iterator[Completion]:
        """
        Yield only namespace segments (paths with sub-commands beneath them).
        Leaf commands are not valid `use` targets.
        """
        active = self._active_prefix()
        scan_prefix = (active + ".") if active else ""
        seen: set[str] = set()

        for cmd in self._spec.commands:
            node = self._spec.commands[cmd]
            if node.bootstrap:
                continue
            if scan_prefix and not cmd.startswith(scan_prefix):
                continue
            rel = cmd[len(scan_prefix) :]
            if not rel.startswith(partial):
                continue
            after = rel[len(partial) :]
            seg = after.split(".")[0]
            insert = partial + seg
            if not seg or insert in seen:
                continue
            full_path = scan_prefix + insert
            has_children = any(
                c.startswith(full_path + ".") and not self._spec.commands[c].bootstrap
                for c in self._spec.commands
            )
            if not has_children:
                continue
            seen.add(insert)
            sub_count = sum(
                1
                for c in self._spec.commands
                if c.startswith(full_path + ".")
                and not self._spec.commands[c].bootstrap
            )
            meta = f"{sub_count} command{'s' if sub_count != 1 else ''}"
            yield Completion(
                text=seg,
                start_position=0,
                display=FormattedText([("class:completion-cmd", insert)]),
                display_meta=meta,
            )

    # ── help <arg> completion ─────────────────────────────────────────────────

    def _complete_help_arg(self, partial: str) -> Iterator[Completion]:
        """
        Yield commands (leaf and namespace) visible from current namespace
        that match `partial`.  Mirrors _complete_cmd_path but emits both
        leaves and namespaces.
        """
        prefix = self._active_prefix()
        seen: set[str] = set()
        ns_dot = (prefix + ".") if prefix else ""

        for cmd in sorted(self._spec.commands):
            node = self._spec.commands[cmd]
            if node.bootstrap:
                continue
            if ns_dot and not cmd.startswith(ns_dot):
                continue
            rel = cmd[len(ns_dot) :]
            if not rel.startswith(partial):
                continue
            suffix = rel[len(partial) :]
            next_seg = suffix.split(".")[0]
            full_seg = partial + next_seg
            if not next_seg and full_seg not in seen:
                # exact match
                full_seg = rel
            if full_seg in seen:
                continue
            seen.add(full_seg)
            meta = node.description[:40] if node.description else ""
            yield Completion(
                text=next_seg or full_seg[len(partial) :],
                start_position=0,
                display=FormattedText([("class:completion-cmd", full_seg)]),
                display_meta=meta,
            )

    # ── Meta-command completion ───────────────────────────────────────────────

    def _complete_meta(self, partial: str, seen: set[str]) -> Iterator[Completion]:
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
        raw_partial: str,
        flag_partial: str,
    ) -> Iterator[Completion]:
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
        self,
        node: CmdNode,
        field_name: str,
        partial: str,
        typed_flags: dict[str, str] | None = None,
    ) -> Iterator[Completion]:
        field = next((f for f in node.input_fields if f.name == field_name), None)
        if field is None:
            return

        # Enum
        if field.enum_values:
            for v in field.enum_values:
                if v.startswith(partial):
                    yield Completion(v[len(partial) :], start_position=0, display=v)
            return

        # Bool
        if field.proto_type == FieldDescriptorProto.TYPE_BOOL:
            for v in ("true", "false"):
                if v.startswith(partial):
                    yield Completion(v[len(partial) :], start_position=0)
            return

        # Integer range — show values only, no meta clutter
        if field.int_completion_end > 0:
            start = field.int_completion_start
            end = field.int_completion_end
            step = field.int_completion_step or 1
            for v in range(start, end + 1, step):
                sv = str(v)
                if sv.startswith(partial):
                    yield Completion(sv[len(partial) :], start_position=0, display=sv)
            return

        # RPC-backed
        comp_src = node.completion_for_field(field_name)
        if comp_src is None:
            return

        # Live: re-fetch synchronously before reading cache.
        # Pass typed_flags so ${ENV} can expand from --env dev in the buffer.
        if comp_src.live and self._live_fetch_fn:
            self._live_fetch_fn(comp_src, typed_flags or {})

        self._last_completion_source = comp_src

        # Re-apply this command's filters at read time.
        # The cache may have been populated by a different command sharing the
        # same source_rpc (e.g. user.get vs user.get.active) without these filters.
        rows = self._cache.get(comp_src.source_rpc)
        if comp_src.filters:
            rows = tuple(
                r for r in rows if all(f.test(r.raw) for f in comp_src.filters)
            )

        # Re-project display_cols using THIS command's display_fields.
        # The cache rows were projected when they were stored — using whichever
        # command first populated the cache.  A command with different display_fields
        # (e.g. user.get.active has 3 cols; user.get has 4) would otherwise render
        # the wrong columns.  Re-projecting from row.raw is cheap and always correct.
        if comp_src.display_fields:
            rows = tuple(
                CandidateRow(
                    insert_value=r.insert_value,
                    display_cols=_render_display_cols(r.raw, comp_src.display_fields),
                    raw=r.raw,
                )
                for r in rows
            )

        for row in rows:
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
    """
    Render the header row for a multi-column completion dropdown.

    Always uses plain text for headers — never a renderer icon.
    If df.label is set to an icon character (non-ASCII, single glyph) rather
    than a real column name, fall back to df.path so the header stays readable.
    """
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


def _extract_typed_flags(words: list[str]) -> dict[str, str]:
    """
    Scan words for --flag value pairs already typed in the buffer.
    Returns a dict with both the original name and its uppercase variant so
    source_request templates like '{"env":"${ENV}"}' expand correctly when
    the user has typed --env dev.

    Example:
      ["config.get", "--env", "dev", "--key"]
      → {"env": "dev", "ENV": "dev"}
    """
    result: dict[str, str] = {}
    i = 1  # skip command word
    while i < len(words) - 1:
        w = words[i]
        if w.startswith("--"):
            name = w.lstrip("-")
            value = words[i + 1]
            if not value.startswith("-"):
                result[name] = value
                result[name.upper()] = value
                i += 2
                continue
        i += 1
    return result
