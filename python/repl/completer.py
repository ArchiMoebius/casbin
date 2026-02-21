"""
repl/completer.py

ReplCompleter: depends only on UISpec + CompletionCache.
No grpc, no proto, no I/O.

HeaderAwareCompleter: thin wrapper that injects non-selectable header rows.
"""
from __future__ import annotations

import shlex
from typing import Iterator, Optional

from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document
from prompt_toolkit.formatted_text import FormattedText

from google.protobuf.descriptor_pb2 import FieldDescriptorProto

from repl.schema.ui_spec import (
    CandidateRow, CellRenderer, CompletionCache, CompletionSource,
    CmdNode, FieldSpec, UISpec,
)


# ──────────────────────────────────────────────────────────────────────────────
# Core completer
# ──────────────────────────────────────────────────────────────────────────────

class ReplCompleter(Completer):
    """
    Provides completions for:
      - Command dot-paths and aliases          (empty buffer or partial command)
      - Flag names  --field                    (after command, before/during a flag)
      - Flag values                            (after --flag or --flag <partial>)
        · enum names
        · bool literals
        · RPC-backed candidates (multi-column display for message types)
    """

    def __init__(self, ui_spec: UISpec, cache: CompletionCache) -> None:
        self._spec  = ui_spec
        self._cache = cache
        self._last_completion_source: Optional[CompletionSource] = None

    # ── Public ───────────────────────────────────────────────────────────────

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        text        = document.text_before_cursor
        after_space = text.endswith(" ")

        try:
            words = shlex.split(text)
        except ValueError:
            words = text.split()

        # ── Top-level: empty buffer or partial command name ───────────────────
        if not words or (len(words) == 1 and not after_space):
            yield from self._complete_cmd_path(words[0] if words else "")
            return

        # ── Resolve command node ──────────────────────────────────────────────
        node = self._spec.find_cmd(words[0])
        if node is None:
            if not after_space:
                yield from self._complete_cmd_path(words[0])
            return

        last = words[-1]  # last *parsed* token

        # ─────────────────────────────────────────────────────────────────────
        # Determine completion context.  Four cases, checked in order:
        #
        #  A  "cmd --flag <TAB>"   after_space=T, last token is --flag
        #     → complete value of that flag, partial=""
        #
        #  B  "cmd --flag val<TAB>"  after_space=F, last token is NOT a flag,
        #     second-to-last IS a flag
        #     → complete partial value
        #
        #  C  "cmd --fl<TAB>"  after_space=F, last token starts with -
        #     → complete flag name, replacing the partial --fl in the buffer
        #
        #  D  everything else (after value, after space with no pending flag)
        #     → offer unused flag names, inserting from scratch
        # ─────────────────────────────────────────────────────────────────────

        # Case A ──────────────────────────────────────────────────────────────
        if after_space and last.startswith("--"):
            field_name = last.lstrip("-")
            yield from self._complete_value(node, field_name, "")
            return

        # Case B ──────────────────────────────────────────────────────────────
        if not after_space and not last.startswith("-") and len(words) >= 3:
            prev = words[-2]
            if prev.startswith("--"):
                field_name = prev.lstrip("-")
                yield from self._complete_value(node, field_name, last)
                return

        # Edge: two words, second is a partial value (no --flag prefix visible)
        # e.g. "cmd val<TAB>" with len(words)==2 — treat as flag name partial
        # only if it starts with -; otherwise fall through to Case D.

        # Case C ──────────────────────────────────────────────────────────────
        # Raw text of the current partial flag token (including dashes).
        # We need this to set start_position correctly so the dashes are
        # preserved when the completion is accepted.
        if not after_space and last.startswith("-"):
            raw_partial   = last          # e.g. "--k" or "--" or "-"
            flag_partial  = last.lstrip("-")  # e.g. "k" or "" or ""
            yield from self._complete_flags(
                node, words, used_current=raw_partial,
                flag_partial=flag_partial, raw_partial=raw_partial,
            )
            return

        # Case D ──────────────────────────────────────────────────────────────
        yield from self._complete_flags(
            node, words, used_current=None,
            flag_partial="", raw_partial="",
        )

    def current_completion_source(
        self, document: Document
    ) -> Optional[CompletionSource]:
        return self._last_completion_source

    # ── Command-path completion ───────────────────────────────────────────────

    def _complete_cmd_path(self, partial: str) -> Iterator[Completion]:
        """
        Trie-aware:
          ""     → one entry per unique top-level segment ("key", "services")
          "key"  → namespace entry "key  (3 commands)"
          "key." → leaf entries "key.get", "key.put", "key.watch"
        """
        seen: set[str] = set()
        show_full = "." in partial
        candidates: list[tuple[str, str, str]] = []

        for cmd, node in self._spec.commands.items():
            if node.bootstrap or not cmd.startswith(partial):
                continue
            if show_full:
                insert  = cmd[len(partial):]
                display = cmd
                meta    = node.description[:40] if node.description else ""
            else:
                remainder = cmd[len(partial):]
                first_seg = remainder.split(".")[0]
                insert    = first_seg
                sub       = self._spec.subtree(partial + first_seg)
                if len(sub) > 1:
                    display = partial + first_seg
                    meta    = f"{len(sub)} commands"
                else:
                    display = cmd
                    meta    = node.description[:40] if node.description else ""
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

        # Aliases (always flat)
        for alias, canonical in self._spec.alias_map.items():
            if not alias.startswith(partial) or alias in seen:
                continue
            node = self._spec.find_cmd(canonical)
            if node is None or node.bootstrap:
                continue
            seen.add(alias)
            yield Completion(
                text=alias[len(partial):],
                start_position=0,
                display=FormattedText([
                    ("class:completion-alias", alias),
                    ("class:completion-sep",   "  "),
                    ("class:completion-meta",  f"→ {canonical}"),
                ]),
                display_meta=node.description[:30] if node.description else "",
            )

    # ── Flag name completion ──────────────────────────────────────────────────

    def _complete_flags(
        self,
        node:          CmdNode,
        words:         list[str],
        used_current:  Optional[str],  # the raw partial token to exclude from used set
        flag_partial:  str,            # the name part after stripping dashes
        raw_partial:   str,            # the full raw token including dashes
    ) -> Iterator[Completion]:
        """
        Yield --flag completions.

        Crucially, `text` is always the *full* "--name" string and
        `start_position` is negative by the length of whatever partial flag
        text is already in the buffer.  This ensures:

          buffer "key.put "    → insert "--key",   start_position=0  → "key.put --key"
          buffer "key.put -"   → insert "--key",   start_position=-1 → "key.put --key"
          buffer "key.put --"  → insert "--key",   start_position=-2 → "key.put --key"
          buffer "key.put --k" → insert "--key",   start_position=-3 → "key.put --key"
        """
        # Build the set of flags that are already fully specified.
        # Skip the token currently being typed (used_current).
        used_flags: set[str] = set()
        for w in words[1:]:
            if w.startswith("--") and w != used_current:
                used_flags.add(w.lstrip("-"))

        start_pos = -len(raw_partial)  # 0 when nothing typed yet

        for fld in node.input_fields:
            if fld.hidden or fld.name in used_flags:
                continue
            if not fld.name.startswith(flag_partial):
                continue
            full_flag = f"--{fld.name}"
            yield Completion(
                text=full_flag,
                start_position=start_pos,
                display=FormattedText([("class:completion-flag", full_flag)]),
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
                        text=v[len(partial):],
                        start_position=0,
                        display=v,
                    )
            return

        # Bool
        if field.proto_type == FieldDescriptorProto.TYPE_BOOL:
            self._last_completion_source = None
            for v in ("true", "false"):
                if v.startswith(partial):
                    yield Completion(v[len(partial):], start_position=0)
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
                text=row.insert_value[len(partial):],
                start_position=0,
                display=_render_candidate_row(row, comp_src.column_separator),
            )


# ──────────────────────────────────────────────────────────────────────────────
# Header-aware wrapper
# ──────────────────────────────────────────────────────────────────────────────

class HeaderAwareCompleter(Completer):
    """
    Prepends a non-insertable styled header row to RPC-backed completions
    when the CompletionSource declares show_headers=True.
    """

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
        w     = df.width or len(label)
        parts.append(("class:completion-header", label.ljust(w)[:w]))
    return FormattedText(parts)


def _type_label(field: FieldSpec) -> str:
    from repl.schema.parser import _proto_type_label
    return _proto_type_label(field)