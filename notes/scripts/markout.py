#!/usr/bin/env python3
"""Extract fenced code blocks from a markdown file, run them in a persistent
shell session (nushell) and insert each block's output in place, between
`<!-- output:TYPE -->` / `<!-- output:end -->` markers.

Usage:

    markout.py FILE [--dry-run] [--no-write]

- Fenced ```nu code blocks are detected and executed in document order.
  Other languages are not supported yet and abort with an error.
- All blocks are fed to a single persistent PTY-backed nushell session, so
  variables and environment (`$env.*`) carry across blocks.
- Output is captured between output markers:

      <!-- output:code -->
      <!-- output:end -->

  where TYPE selects the fence language used when rendering the output:
  plain `output:` (or `output:markdown`) inserts raw markdown without a
  fence. Blocks without output markers are still executed (state setup,
  cleanups, ...) but their output is discarded.
- `<!-- input:NAME -->` / `<!-- input:end -->` regions hold one fenced code
  block whose content is materialized verbatim as a file named NAME, right
  before the first `nu` block that follows the marker (re-declaring the
  same NAME materializes the file again with the new content). This keeps
  inline scripts (e.g. `python -c "..."` payloads) readable in the document
  while `nu` blocks stay short runners. The file is written under the path
  given by the most recent `<!-- cwd:PATH -->` marker, or the process CWD
  when no `cwd` marker has been seen yet — place `cwd`/input markers after
  any block that wipes the target directory:

      <!-- cwd:.tmp/scratch -->
      <!-- input:helper.py -->
      ```python
      print("hello")
      ```
      <!-- input:end -->

      ```nu
      python ($scratch + "/helper.py")
      ```
- Code blocks and captured output are syntax highlighted when printed to a
  terminal (pygments, optional — plain text if not installed).
- The process CWD (usually the repository root) is stripped from captured
  output, so tools printing absolute paths (e.g. `Initializing auth server
  keys: <cwd>/var/instances/...`) don't embed machine-specific paths into
  the document — paths come out CWD-relative. Pass `--keep-cwd` to disable.
- A sentinel command is sent after every block, so block completion is
   detected reliably even if the block fails; a failed block is reported and
   the run stops at the first failing block, the file is not written when
   any block fails.

The script must be run from the same working directory as the shell commands
expect (usually the repository root). Modified file is written back in place.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import pty
import re
import select
import signal
import struct
import sys
import termios
import time

LANGS = {"nu"}  # fence languages supported by the persistent session

# Output marker: <!-- output:TYPE -->, TYPE is optional, defaults to markdown.
# `output:end` is the closing marker and must not match as an opening one.
OUTPUT_OPEN = re.compile(r"^\s*<!--\s*output:(?!end\b)(?P<type>[A-Za-z0-9_-]*)\s*-->\s*$")
OUTPUT_CLOSE = re.compile(r"^\s*<!--\s*output:end\s*-->\s*$")

# Input materialization: <!-- cwd:PATH --> sets the directory for the
# <!-- input:NAME --> / <!-- input:end --> regions that follow it (defaults
# to the process CWD). Each input region holds exactly one fenced code block
# whose content is written verbatim to PATH/NAME before `nu` blocks run.
CWD_OPEN = re.compile(r"^\s*<!--\s*cwd:(?P<path>.+?)\s*-->\s*$")
INPUT_OPEN = re.compile(r"^\s*<!--\s*input:(?!end\b)(?P<name>[A-Za-z0-9_./-]+)\s*-->\s*$")
INPUT_CLOSE = re.compile(r"^\s*<!--\s*input:end\s*-->\s*$")

# ANSI escape sequences: CSI, OSC (with BEL or ST terminator), simple
# two-byte escapes and other single-byte escapes.
ANSI_RE = re.compile(
    rb"\x1b(?:"
    rb"\[[0-9;?<>=!]*[a-zA-Z@`]"  # CSI
    rb"|\][^\x07\x1b]*(?:\x07|\x1b\\)"  # OSC, e.g. OSC 133 prompts
    rb"|[PX^_][^\x07\x1b]*(?:\x07|\x1b\\)"  # DCS/SOS/PM/APC
    rb"|[ -/]+."  # two-byte sequences
    rb"|[0-9A-Za-z=><\\]"  # single-byte sequences
    rb")"
)

# OSC 133 shell integration markers emitted around command execution:
# `C` opens the command output region, `D;<exit code>` closes it.
OSC133_OUTPUT_START = re.compile(rb"\x1b\]133;C[^\x07\x1b]*(?:\x07|\x1b\\)")
OSC133_OUTPUT_END = re.compile(rb"\x1b\]133;D(?:;(\d+))?[^\x07\x1b]*(?:\x07|\x1b\\)")

# Cursor position request from reedline, answered with a fake position.
# Reedline blocks on it until it is answered, so missing one (e.g. a request
# split across two reads) wedges the whole session.
CPR_REQUEST = b"\x1b[6n"
CPR_RESPONSE = b"\x1b[1;1R"

# Syntax highlighting, optional, needs pygments — falls back to plain text.
try:
    import pygments
    from pygments.formatters import Terminal256Formatter
    from pygments.lexer import RegexLexer, inherit
    from pygments.lexers import get_lexer_by_name
    from pygments.token import (
        Comment,
        Keyword,
        Name,
        Number,
        Operator,
        Punctuation,
        String,
        Whitespace,
    )
    from pygments.util import ClassNotFound

    class NushellLexer(RegexLexer):
        """Minimal nushell lexer (pygments has no built-in one)."""

        name = "Nushell"
        aliases = ["nu", "nushell"]
        filenames = ["*.nu"]

        tokens = {
            "root": [
                (r"\s+", Whitespace),
                (r"#.*$", Comment.Single),
                (r'\$(?:"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\')', String.Interpol),
                (r'"(?:[^"\\]|\\.)*"', String.Double),
                (r"'[^']*'", String.Single),
                (r"`[^`]*`", String.Backtick),
                (r"\$\w[\w.-]*", Name.Variable),
                (r"--[a-zA-Z][\w-]*", Keyword.Type),
                (r"-(?![\d.])[a-zA-Z]\b", Keyword.Type),
                (r"\b\d{4}-\d{2}-\d{2}(?:T[\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)?\b", Number),
                (r"\b(?:\d[\d_]*(?:\.[\d_]+)?|\.\d+)(?:[eE][+-]?\d+)?(?:[a-zA-Z]+)?(?![\w.])", Number),
                (r"\b(?:true|false|null|inf|nan)\b", Keyword.Constant),
                (
                    r"\b(?:let|mut|const|def|export|use|hide|module|extern|alias|source|overlay"
                    r"|if|else|match|try|catch|do|for|while|loop|break|continue|return|error)\b",
                    Keyword,
                ),
                (r"\b(?:in|not|and|or|mod)\b", Operator.Word),
                (r"\b[a-zA-Z_][\w-]*\b", Name),
                (r"\^[^\s;|]+", Name.Other),
                (r"\.\.<|\.\.|\*\*|//|==|!=|<=|>=|=~|!~|=>", Operator),
                (r"[=<>+\-*/]", Operator),
                (r"[|;]", Operator),
                (r"[\[\]{}(),:.]", Punctuation),
                (r".", Punctuation),
            ]
        }

    class NushellOutputLexer(NushellLexer):
        """Nushell lexer variant for captured output: table border characters
        are dimmed, so data stands out."""

        name = "Nushell output"
        aliases = ["nu-output"]

        tokens = {
            "root": [
                (r"^[─│╭╮╰╯┬┴┼├┤┌┐└┘╞╡═║╤╧╪╫╬]+[─│╭╮╰╯┬┴┼├┤┌┐└┘╞╡═║╤╧╪╫╬\s]*$", Comment),
                inherit,
            ]
        }

    _FORMATTER = Terminal256Formatter()
    _LEXERS: dict = {"nu": NushellLexer(), "nu-output": NushellOutputLexer()}

    def _lexer(lang: str):
        if lang not in _LEXERS:
            try:
                _LEXERS[lang] = get_lexer_by_name(lang)
            except ClassNotFound:
                _LEXERS[lang] = NushellLexer()
        return _LEXERS[lang]

except ImportError:
    pygments = None


def strip_cwd(output: str) -> str:
    """Remove the process CWD (with a trailing separator) from captured
    output, turning absolute paths that start with it into CWD-relative
    paths — see the module docstring."""
    cwd = os.getcwd()
    if not cwd.endswith(os.sep):
        cwd += os.sep
    return output.replace(cwd, "")


def highlight(text: str, lang: str = "nu") -> str:
    """Syntax highlight text for terminal display, plain text if pygments is
    not installed or stdout is not a terminal."""
    if pygments is None or not sys.stdout.isatty():
        return text
    return pygments.highlight(text, _lexer(lang), _FORMATTER)


class MarkerError(Exception):
    pass


class ExecError(Exception):
    pass


class Block:
    def __init__(self, start: int, fence: str, lang: str, lines: list[str], end: int):
        self.start = start  # line number of the opening fence (0-based)
        self.fence = fence  # opening fence, e.g. "```nu"
        self.lang = lang  # fence info string, e.g. "nu"
        self.lines = lines  # block content lines
        self.end = end  # line number of the closing fence
        self.output_type: str | None = None  # output type from markers
        self.open_line: int | None = None  # <!-- output:... --> line
        self.close_line: int | None = None  # <!-- output:end --> line
        self.output: str | None = None  # captured output (markdown lines)

    @property
    def has_markers(self) -> bool:
        return self.open_line is not None

    @property
    def code(self) -> str:
        return "\n".join(self.lines) + "\n"

    def output_markdown(self) -> list[str]:
        """Render captured output as markdown lines to insert into the file."""
        if self.output is None:
            raise MarkerError(f"block at line {self.start + 1} has no output")
        if self.output_type in ("", "markdown"):
            return self.output.rstrip("\n").splitlines()
        lang = "" if self.output_type == "code" else self.output_type
        lines = self.output.rstrip("\n").splitlines()
        return [f"```{lang}", *lines, "```"]


def parse_blocks(text: str) -> list[Block]:
    """Find fenced code blocks and pair them with following output markers."""
    lines = text.splitlines()
    # collect output marker regions first, fences inside a marker region are
    # rendered output (e.g. ```code fences), not executable code blocks;
    # input regions are excluded the same way — their fenced blocks are
    # materialized as files, not executed
    regions: list[tuple[int, int, str]] = []  # (open line, close line, type)
    excluded: list[tuple[int, int]] = []  # (open line, close line)
    i = 0
    while i < len(lines):
        m = OUTPUT_OPEN.match(lines[i])
        if m:
            open_line = i
            j = i + 1
            while j < len(lines):
                if OUTPUT_CLOSE.match(lines[j]):
                    break
                j += 1
            if j == len(lines):
                raise MarkerError(f"output marker at line {open_line + 1} is missing `<!-- output:end -->`")
            regions.append((open_line, j, m.group("type") or "markdown"))
            excluded.append((open_line, j))
            i = j + 1
            continue
        m = INPUT_OPEN.match(lines[i])
        if m:
            j = i + 1
            while j < len(lines):
                if INPUT_CLOSE.match(lines[j]):
                    break
                j += 1
            if j == len(lines):
                raise MarkerError(f"input marker at line {i + 1} is missing `<!-- input:end -->`")
            excluded.append((i, j))
            i = j + 1
            continue
        i += 1
    blocks: list[Block] = []
    i = 0
    while i < len(lines):
        m = re.match(r"^(\s*)([`]{3,}|[~]{3,})\s*([A-Za-z0-9_-]*)", lines[i])
        if not m or any(open_line < i < close_line for open_line, close_line in excluded):
            i += 1
            continue
        fence = m.group(2)
        fence_char = fence[0]
        fence_len = len(fence)
        if m.group(3):
            # opening fence with a language/info string
            lang = m.group(3)
            indent = m.group(1)
            content: list[str] = []
            j = i + 1
            while j < len(lines):
                if re.match(rf"^{re.escape(indent)}{fence_char}{{{fence_len},}}\s*$", lines[j]):
                    break
                content.append(lines[j])
                j += 1
            if j == len(lines):
                raise MarkerError(f"unclosed code fence at line {i + 1}")
            block = Block(i, lines[i], lang, content, j)
            blocks.append(block)
            i = j + 1
        else:
            # closing fence (or bare fence without language) — skip its content
            i += 1
            while i < len(lines):
                if re.match(rf"^\s*{fence_char}{{{fence_len},}}\s*$", lines[i]):
                    i += 1
                    break
                i += 1
    # pair each block with the first output marker region that follows it —
    # markers are consumed in document order, one per block
    for bi, block in enumerate(blocks):
        after = block.end
        next_block = blocks[bi + 1].start if bi + 1 < len(blocks) else len(lines)
        for open_line, close_line, output_type in regions:
            if after < open_line < next_block:
                block.open_line = open_line
                block.close_line = close_line
                block.output_type = output_type
                break
    return blocks


class Input:
    def __init__(self, name: str, lang: str, lines: list[str], cwd: str | None, open_line: int):
        self.name = name  # file name passed in the input marker, e.g. "main.py"
        self.lang = lang  # fence info string, e.g. "python"
        self.lines = lines  # block content lines
        self.cwd = cwd  # path from the most recent <!-- cwd:... --> marker
        self.open_line = open_line  # line number of the <!-- input:... --> marker (0-based)

    @property
    def code(self) -> str:
        return "\n".join(self.lines) + "\n"

    @property
    def path(self) -> str:
        """Absolute (or CWD-relative) path the content is materialized at."""
        base = self.cwd if self.cwd is not None else os.getcwd()
        return os.path.join(base, self.name)


def parse_inputs(text: str) -> list[Input]:
    """Find `<!-- input:NAME -->` regions, each holding one fenced block;
    resolve the materialization directory for each from the most recent
    preceding `<!-- cwd:PATH -->` marker (or the process CWD)."""
    lines = text.splitlines()
    inputs: list[Input] = []
    cwd: str | None = None
    i = 0
    while i < len(lines):
        cm = CWD_OPEN.match(lines[i])
        if cm:
            cwd = os.path.expanduser(cm.group("path"))
            i += 1
            continue
        im = INPUT_OPEN.match(lines[i])
        if im:
            open_line = i
            # find the first fenced opening fence inside the region
            j = i + 1
            fence_info: tuple[str, str, int, str] | None = None  # (indent, fence chars, len, lang)
            while j < len(lines):
                fm = re.match(r"^(\s*)([`]{3,}|[~]{3,})\s*([A-Za-z0-9_-]*)", lines[j])
                if fm and fm.group(3):
                    fence_info = (fm.group(1), fm.group(2), len(fm.group(2)), fm.group(3))
                    break
                if fm and not fence_info:
                    raise MarkerError(f"input at line {open_line + 1} has a bare fence without a language")
                j += 1
            if not fence_info:
                raise MarkerError(f"input at line {open_line + 1} has no fenced code block")
            indent, fence, fence_len, lang = fence_info
            j += 1
            content: list[str] = []
            while j < len(lines):
                if re.match(rf"^{re.escape(indent)}{re.escape(fence[0])}{{{fence_len},}}\s*$", lines[j]):
                    break
                content.append(lines[j])
                j += 1
            if j == len(lines):
                raise MarkerError(f"input at line {open_line + 1} has an unclosed code fence")
            close_line = next((k for k in range(j, len(lines)) if INPUT_CLOSE.match(lines[k])), None)
            if close_line is None:
                raise MarkerError(f"input at line {open_line + 1} is missing `<!-- input:end -->`")
            inputs.append(Input(im.group("name"), lang, content, cwd, open_line))
            i = close_line + 1
            continue
        i += 1
    return inputs


class Session:
    """Persistent command session in a PTY, driven by a REPL shell."""

    def __init__(self, cmd: list[str]):
        self.sentinel_counter = 0
        self.pid, self.master = pty.fork()
        if self.pid == 0:
            try:
                os.execvp(cmd[0], cmd)
            except OSError:
                os._exit(127)
        self._set_winsize()
        self._cpr_carry = b""
        self._startup()

    def _answer_cpr(self, data: bytes):
        """Answer cursor position requests (`ESC[6n`) from the shell.

        The shell requests the cursor position once its interactive loop is
        up (and again later on prompt redraws) — in a PTY without a real
        terminal emulator such a request is never answered and the shell
        blocks, so answer it. The request is searched in `data` prepended
        with the tail of the previous read, so requests split across two
        reads are answered too.
        """
        chunk = self._cpr_carry + data
        count = chunk.count(CPR_REQUEST)
        if count:
            os.write(self.master, CPR_RESPONSE * count)
            chunk = chunk.replace(CPR_REQUEST, b"")
        # A partial request can only be the last few bytes of the stream.
        self._cpr_carry = chunk[-3:]

    def _startup(self, timeout: float = 10.0):
        """Wait until the REPL is ready for input: nushell requests the
        cursor position once its interactive loop is up — answer it, drain
        the rest of the startup output (banner, prompt render)."""
        start = time.time()
        buf = b""
        while time.time() - start < timeout:
            r, _, _ = select.select([self.master], [], [], 0.5)
            if not r:
                break
            try:
                data = os.read(self.master, 65536)
            except OSError:
                break
            if not data:
                break
            buf += data
            self._answer_cpr(data)
            if CPR_REQUEST in data:
                break
        if os.waitpid(self.pid, os.WNOHANG) != (0, 0):
            raise ExecError("failed to start session")
        self._drain(0.5)

    def _set_winsize(self):
        fcntl.ioctl(self.master, termios.TIOCSWINSZ, struct.pack("HHHH", 50, 240, 0, 0))

    def _drain(self, timeout: float = 0.5) -> bytes:
        buf = b""
        start = time.time()
        while time.time() - start < timeout:
            r, _, _ = select.select([self.master], [], [], 0.1)
            if r:
                try:
                    data = os.read(self.master, 65536)
                except OSError:
                    break
                if not data:
                    break
                buf += data
                self._answer_cpr(data)
        return buf

    def _next_sentinel(self) -> str:
        self.sentinel_counter += 1
        return f"MARKOUT_SENTINEL_{self.sentinel_counter}_0_0_"

    def run(self, code: str, timeout: float) -> tuple[str, int | None]:
        """Run a code chunk, return its output and the chunk exit code."""
        sentinel = self._next_sentinel()
        # Nushell (reedline) bracketed paste: the whole chunk is inserted as
        # one buffer, then a single Enter (\r) runs it as one multi-line
        # command. A trailing newline inside the paste is required for nu to
        # accept the paste content into the buffer.
        #
        # The sentinel is sent as a separate REPL entry right after the paste:
        # even if the chunk fails (`error make` aborts the rest of its own
        # buffer), the sentinel entry still executes, so completion is always
        # detected. The chunk itself is bracketed by the OSC 133 output
        # markers (`C` ... `D;<exit code>`), which is what gets extracted.
        payload = f'\x1b[200~{code}\n\x1b[201~\rprint "{sentinel}"\r'
        os.write(self.master, payload.encode())
        buf = self._wait_for(sentinel, timeout)
        return self._extract_output(buf)

    def _wait_for(self, sentinel: str, timeout: float) -> bytes:
        """Read until the sentinel entry completes: the sentinel is first
        echoed as it is typed, then printed by the executed `print` command
        (its 2nd occurrence in the cleaned stream), followed by the OSC 133
        prompt-end marker."""
        start = time.time()
        buf = b""
        sb = sentinel.encode()
        while time.time() - start < timeout:
            # completion can be detected from already-buffered bytes alone,
            # so check unconditionally on every iteration, not only when new
            # bytes arrive
            if ANSI_RE.sub(b"", buf).count(sb) >= 2 and b"\x1b]133;D" in buf:
                return buf
            r, _, _ = select.select([self.master], [], [], 0.2)
            if r:
                try:
                    data = os.read(self.master, 65536)
                except OSError:
                    break
                if not data:
                    break
                buf += data
                self._answer_cpr(data)
                # first occurrence — the command echo; keep reading until the
                # sentinel print output arrives; the tail of the echo (prompt
                # redraw) may lag behind the first read chunk
                if ANSI_RE.sub(b"", buf).count(sb) < 2:
                    buf += self._drain(0.05)
        if os.waitpid(self.pid, os.WNOHANG) != (0, 0):
            raise ExecError("session exited unexpectedly")
        raise ExecError(f"timed out after {timeout}s waiting for output")

    def _extract_output(self, buf: bytes) -> tuple[str, int | None]:
        """Extract the chunk output from the session output.

        The chunk output lies between the OSC 133 output-start (`C`) and
        output-end (`D;<exit code>`) markers of the chunk's own execution —
        everything else (paste echo, reedline renders, the sentinel entry and
        prompt redraws) is outside this region.
        """
        start = OSC133_OUTPUT_START.search(buf)
        end = OSC133_OUTPUT_END.search(buf, start.end()) if start else None
        if not start or not end:
            raise ExecError("can't find chunk output in session output")
        exit_code = int(end.group(1)) if end.group(1) else None
        buf = buf[start.end() : end.start()]
        clean = ANSI_RE.sub(b"", buf)
        text = clean.decode("utf-8", "replace")
        # normalize `\r\n` (PTY line discipline) and strip leftover `\r`
        text = text.replace("\r\n", "\n").replace("\r", "")
        return text.rstrip("\n"), exit_code

    def close(self):
        # Terminate the whole process group (nu is a session and group
        # leader, `pty.fork` calls setsid), so jobs spawned by code blocks
        # (e.g. a running Spinta server) do not outlive the session.
        try:
            os.killpg(self.pid, signal.SIGTERM)
            try:
                os.waitpid(self.pid, 0)
            except ChildProcessError:
                pass
        except ProcessLookupError:
            try:
                os.kill(self.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        os.close(self.master)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", help="markdown file to process")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="parse the file and report blocks, do not execute anything",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="do not write the file back (useful with --dry-run)",
    )
    parser.add_argument(
        "--keep-cwd",
        action="store_true",
        help="keep the absolute process CWD in captured output (by default it is stripped, see the module docstring)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="per-block execution timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--shell",
        default="nu",
        help="nushell binary used to run code blocks (default: %(default)s)",
    )
    args = parser.parse_args()

    with open(args.file, encoding="utf-8") as f:
        text = f.read()

    try:
        blocks = parse_blocks(text)
        inputs = parse_inputs(text)
    except MarkerError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(f"{len(blocks)} code block(s) found in {args.file}")
        for block in blocks:
            markers = f"output:{block.output_type}" if block.has_markers else "no output"
            print(f"  line {block.start + 1:>4}: ```{block.lang}``` ({markers})")
        if inputs:
            print(f"{len(inputs)} input block(s) found")
            for inp in inputs:
                print(f"  {inp.path}: ```{inp.lang}```")
        return 0

    unknown = sorted({block.lang for block in blocks} - LANGS)
    if unknown:
        print(
            f"error: unsupported code block languages: {unknown} (supported: {sorted(LANGS)})",
            file=sys.stderr,
        )
        return 1

    cmd = [args.shell, "--no-config-file", "-i"]
    session = Session(cmd)

    # each input region is materialized once, right before the first code
    # block that follows it (place markers after any block that wipes the
    # target directory); re-declaring the same name re-materializes it
    materialized: set[int] = set()

    lines = text.splitlines()
    failures: list[str] = []
    executed = 0
    succeeded = 0
    for block in blocks:
        # inputs whose markers appear before this block and have not been
        # materialized yet are written now, in document order
        pending_inputs = [i for i, inp in enumerate(inputs) if i not in materialized and inp.open_line < block.start]
        for i in pending_inputs:
            inp = inputs[i]
            materialized.add(i)
            directory = os.path.dirname(inp.path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(inp.path, "w", encoding="utf-8") as f:
                f.write(inp.code)
            print(f"== input file: {inp.path}")
            for line in highlight(inp.code, inp.lang).splitlines():
                print(f" | {line}")
            print()
        print(f"== running block at line {block.start + 1} ({block.lang})")
        for line in highlight(block.code, block.lang).splitlines():
            print(f" | {line}")
        print()
        executed += 1
        try:
            output, exit_code = session.run(block.code, timeout=args.timeout)
        except ExecError as e:
            print(f"error: block at line {block.start + 1} failed: {e}", file=sys.stderr)
            failures.append(f"block at line {block.start + 1}: {e}")
            break
        if not args.keep_cwd:
            output = strip_cwd(output)
        error_lines = output.splitlines()
        # exit code comes from the OSC 133 `D;<code>` marker; nu prints
        # errors as an `Error: ...` block, the session survives and
        # continues with the next block
        failed = bool(exit_code) or (exit_code is None and any(line.startswith("Error:") for line in error_lines))
        if failed:
            print(f"error: block at line {block.start + 1} failed:", file=sys.stderr)
            print("\n".join(error_lines), file=sys.stderr)
            failures.append(f"line {block.start + 1}")
            break
        succeeded += 1
        if block.has_markers:
            out_lang = {"code": "nu-output", "markdown": "markdown"}.get(
                block.output_type, block.output_type or "nu-output"
            )
        else:
            out_lang = "nu-output"
        print(highlight(output, out_lang))
        print()
        if block.has_markers:
            block.output = output
    session.close()

    failed = executed - succeeded
    skipped = len(blocks) - executed
    print(
        f"\nsummary: {len(blocks)} block(s) total, {executed} executed, "
        f"{succeeded} succeeded, {failed} failed, {skipped} skipped"
    )

    if failures:
        print(f"\n{len(failures)} code block(s) failed:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    # insert outputs between markers, from the bottom up, so line numbers
    # above don't shift while we edit
    changed = False
    for block in reversed(blocks):
        if not block.has_markers:
            continue
        out_lines = block.output_markdown()
        if block.close_line is None or block.open_line is None:
            continue
        if lines[block.open_line + 1 : block.close_line] != out_lines:
            lines[block.open_line + 1 : block.close_line] = out_lines
            changed = True

    if changed and not args.no_write:
        with open(args.file, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print(f"updated {args.file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
