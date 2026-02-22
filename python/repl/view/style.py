"""repl/view/style.py — prompt_toolkit Style + key bindings."""

from __future__ import annotations

from prompt_toolkit.styles import Style
from prompt_toolkit.key_binding import KeyBindings


repl_style = Style.from_dict(
    {
        # Prompt chrome
        "server": "#5f87af",
        "service": "#87af87 bold",
        "arrow": "#ffffff",
        # Completion menu
        "completion-menu.completion": "bg:#1e1e2e #cdd6f4",
        "completion-menu.completion.current": "bg:#313244 #cba6f7 bold",
        "completion-menu.meta": "#6c7086",
        "completion-menu.meta.current": "#a6adc8",
        # Projected completion columns
        "completion-meta": "#6c7086",
        "completion-dim": "#45475a italic",
        "completion-sep": "#313244",
        # Completion header row (column labels)
        "completion-header": "#89b4fa bold underline",
        "completion-header-sep": "#313244",
        "completion-header-row": "bg:#181825",
        # Command / alias styling inside completion menu
        "completion-cmd": "#cdd6f4",
        "completion-alias": "#a6e3a1",
        # Auto-suggest ghost text
        "auto-suggestion": "#585b70 italic",
        # Validation toolbar
        "validation-toolbar": "bg:#f38ba8 #1e1e2e",
        # Bottom toolbar
        "bottom-toolbar": "bg:#181825 #6c7086",
        "bottom-toolbar.text": "#6c7086",
        # Wizard prompt
        "wizard-field": "#a6e3a1 bold",
        "wizard-type": "#6c7086 italic",
        # Output
        "success": "#a6e3a1",
        "error": "#f38ba8",
        "stream-index": "#6c7086",
        "stream-sep": "#313244",
    }
)


def build_key_bindings(
    completer=None,
    is_streaming_fn=None,
    cancel_fn=None,
) -> KeyBindings:
    kb = KeyBindings()

    @kb.add("c-c")
    def _interrupt(event):
        """Ctrl-C: cancel an active stream, or clear the input line."""
        if is_streaming_fn and is_streaming_fn() and cancel_fn:
            cancel_fn()
        else:
            event.app.current_buffer.reset()

    @kb.add("tab")
    def _tab(event):
        """
        Tab behaviour:
          - Menu already open → cycle to the next completion (select_first
            style cycling: Tab moves forward through candidates).
          - Menu not open     → open it.  select_first=True means the first
            candidate is immediately highlighted so the next Tab cycles from
            item 2 onward.  Works on an empty buffer too.
        """
        buf = event.app.current_buffer
        if buf.complete_state:
            # Menu is visible — advance to next item
            buf.complete_next()
        else:
            # Open menu and pre-select first item so Tab immediately cycles
            buf.start_completion(select_first=True)

    @kb.add("f1")
    def _help(event):
        """F1: submit 'help' for the command currently being typed."""
        buf = event.app.current_buffer
        word = buf.text.split(" ")[0] if buf and buf.text.strip() else ""
        buf.text = f"help {word}".strip()
        buf.validate_and_handle()

    return kb
