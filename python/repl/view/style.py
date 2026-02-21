"""repl/view/style.py — prompt_toolkit Style + key bindings."""
from __future__ import annotations

from prompt_toolkit.styles import Style
from prompt_toolkit.key_binding import KeyBindings


repl_style = Style.from_dict({
    # Prompt chrome
    "server":  "#5f87af",
    "service": "#87af87 bold",
    "arrow":   "#ffffff",

    # Completion menu
    "completion-menu.completion":             "bg:#1e1e2e #cdd6f4",
    "completion-menu.completion.current":     "bg:#313244 #cba6f7 bold",
    "completion-menu.meta":                   "#6c7086",
    "completion-menu.meta.current":           "#a6adc8",

    # Projected completion columns
    "completion-meta":        "#6c7086",
    "completion-dim":         "#45475a italic",
    "completion-sep":         "#313244",

    # Completion header row (column labels)
    "completion-header":      "#89b4fa bold underline",
    "completion-header-sep":  "#313244",
    "completion-header-row":  "bg:#181825",

    # Command / alias styling inside completion menu
    "completion-cmd":         "#cdd6f4",
    "completion-alias":       "#a6e3a1",

    # Auto-suggest ghost text
    "auto-suggestion":        "#585b70 italic",

    # Validation toolbar
    "validation-toolbar":     "bg:#f38ba8 #1e1e2e",

    # Bottom toolbar
    "bottom-toolbar":         "bg:#181825 #6c7086",
    "bottom-toolbar.text":    "#6c7086",

    # Wizard prompt
    "wizard-field":           "#a6e3a1 bold",
    "wizard-type":            "#6c7086 italic",

    # Output
    "success":                "#a6e3a1",
    "error":                  "#f38ba8",
    "stream-index":           "#6c7086",
    "stream-sep":             "#313244",
})


def build_key_bindings(completer=None) -> KeyBindings:
    kb = KeyBindings()

    @kb.add("c-c")
    def _interrupt(event):
        """Ctrl-C: cancel stream or clear line."""
        event.app.current_buffer.reset()

    @kb.add("tab")
    def _tab(event):
        """
        Tab behaviour:
          - Empty buffer (or whitespace only) → open the completion menu
            showing all top-level command groups.  This is the "what can I
            do?" gesture.
          - Non-empty buffer → normal word completion.

        prompt_toolkit's default Tab handler only fires start_completion when
        complete_while_typing=False and there is already text; by registering
        our own handler we make Tab work unconditionally, including on an
        empty buffer.
        """
        buf = event.app.current_buffer
        # start_completion on an empty buffer with complete_while_typing=False
        # would normally do nothing — calling it explicitly forces the menu open.
        buf.start_completion(select_first=False)

    @kb.add("f1")
    def _help(event):
        """F1: submit 'help' for the command currently being typed."""
        buf  = event.app.current_buffer
        word = buf.text.split()[0] if buf.text.strip() else ""
        buf.set_text(f"help {word}".strip())
        buf.validate_and_handle()

    return kb