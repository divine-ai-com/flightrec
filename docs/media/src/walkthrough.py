"""Composes the flightrec walkthrough: one animated SVG of scenes."""

import json
import sys

from kit import *  # noqa: F401,F403
import diagrams

FACTS = json.load(open(sys.argv[2])) if len(sys.argv) > 2 else {}


def scene(d, a, b, draw):
    """Draws a scene visible from a to b seconds (fading at both ends), with
    its own animation times starting at a."""
    d.offset = 0.0
    k = d.keyframes([(0, "opacity:0;visibility:hidden"), (max(a - 0.01, 0), "opacity:0;visibility:hidden"), (a, "opacity:0;visibility:visible"),
                     (a + 0.6, "opacity:1;visibility:visible"), (b - 0.6, "opacity:1;visibility:visible"), (b, "opacity:0;visibility:hidden"),
                     (d.loop, "opacity:0;visibility:hidden")])
    d.add(f'<g class="{k}">')
    d.offset = a
    draw(d, b - a)
    d.add("</g>")
    d.offset = 0.0


def caption(d, s, t0, t1, y=700):
    k = d.show(t0, t1)
    d.add(f'<g class="{k}"><rect x="{640 - len(s)*4.6 - 24:.0f}" y="{y-26}" width="{len(s)*9.2 + 48:.0f}" height="36" rx="18" fill="#000" fill-opacity=".55"/>'
          f'<text x="640" y="{y-2}" fill="#f8fafc" font-size="18" text-anchor="middle">{escape(s)}</text></g>')


def title_card(d, dur):
    d.add(f'<g class="{d.pop(0.3)}">')
    d.add(f'<rect x="390" y="190" width="500" height="150" rx="20" fill="none" stroke="{C["red"]}" stroke-width="4" stroke-dasharray="14 9" class="flow"/>')
    text(d, 640, 290, "flightrec", 96, "#f8fafc", weight="800")
    d.add("</g>")
    text(d, 640, 400, "Debug logs for the requests that fail.", 32, "#e2e8f0", weight="600", cls=d.show(1.2))
    text(d, 640, 444, "One line for the ones that don't.", 32, "#94a3b8", weight="600", cls=d.show(2.0))
    for i, (s, col) in enumerate([("log/slog handler for Go", C["blue2"]), ("zero dependencies", C["green"]), ("HTTP · gRPC · OpenTelemetry", C["purple"])]):
        pill(d, 360 + i * 280, 530, s, col, cls=d.pop(3.0 + i * .4), size=17)


def problem(d, dur):
    background(d, "The Problem", "Every team picks one of two bad options", bg=False)
    # left: debug on
    panel(d, 90, 170, 520, 400, C["orange"], "Debug logging ON", C["orange"], cls=d.show(0.4))
    d.add(f'<g class="{d.show(0.6)}">')
    d.add('<clipPath id="fire"><rect x="120" y="210" width="460" height="230" rx="8"/></clipPath>')
    d.add('<g clip-path="url(#fire)">')
    k = d.uid("scr")
    d.css.append(f"@keyframes {k}{{from{{transform:translateY(0)}}to{{transform:translateY(-312px)}}}} .{k}{{animation:{k} 2.6s linear infinite}}")
    d.add(f'<g class="{k}">')
    import random
    rnd = random.Random(7)
    for i in range(48):
        w = rnd.randint(140, 440)
        col = rnd.choice(["#94a3b8", "#94a3b8", "#cbd5e1", "#7dd3fc"])
        d.add(f'<rect x="130" y="{214 + i*13}" width="{w}" height="8" rx="4" fill="{col}"/>')
    d.add("</g></g>")
    text(d, 350, 480, "20,010 lines · 3.69 MB", 26, C["ink"], weight="800")
    text(d, 350, 512, "per 1,000 requests, 99% of them healthy", 16, C["muted"])
    text(d, 350, 545, "storage bills, noise, slower search", 16, C["red"], weight="700")
    d.add("</g>")
    # right: debug off
    panel(d, 670, 170, 520, 400, C["blue"], "Debug logging OFF", cls=d.show(1.4))
    d.add(f'<g class="{d.show(1.6)}">')
    d.add(f'<rect x="700" y="210" width="460" height="230" rx="8" fill="{C["term"]}"/>')
    text(d, 716, 248, '{"level":"INFO","msg":"request completed"}', 13, "#64748b", anchor="start", mono=True)
    text(d, 716, 272, '{"level":"INFO","msg":"request completed"}', 13, "#64748b", anchor="start", mono=True)
    d.add(f'<g class="{d.show(3.2)}">')
    text(d, 716, 300, '{"level":"ERROR","msg":"payment failed",', 13, "#fca5a5", anchor="start", mono=True)
    text(d, 716, 320, ' "status":502}', 13, "#fca5a5", anchor="start", mono=True)
    d.add("</g>")
    d.add(f'<g class="{d.pop(4.4)}">')
    text(d, 930, 390, "? ? ?", 46, C["amber"], weight="800")
    d.add("</g>")
    text(d, 930, 480, "what happened before the error?", 22, C["ink"], weight="800")
    text(d, 930, 512, "the context is gone, and the bug", 16, C["muted"])
    text(d, 930, 545, "may not happen again", 16, C["red"], weight="700")
    d.add("</g>")
    caption(d, "You need debug logs for the 1% of requests that fail; you don't know which ones in advance.", 6.0, dur - 0.6, y=640)


def adopt(d, dur):
    background(d, "Adopt It in Three Changes", "Existing log calls stay as they are", bg=False)
    lines = [
        ("rec := flightrec.New(flightrec.Options{", "#e2e8f0", True),
        ("    Next:          slog.NewJSONHandler(os.Stdout, nil), // your handler", "#94a3b8", False),
        ("    PassLeveler:   slog.LevelWarn,  // written immediately", "#94a3b8", False),
        ("    BufferLeveler: slog.LevelDebug, // buffered per request", "#94a3b8", False),
        ("})", "#e2e8f0", False),
        ("slog.SetDefault(slog.New(rec))", "#e2e8f0", True),
        ("http.ListenAndServe(\":8080\", flightrec.HTTPMiddleware(rec)(mux))", "#e2e8f0", True),
        ("", "#e2e8f0", False),
        ("stop, _ := rec.StartJournal(\"/var/run/flightrec/journal\") // survive kill -9", C["lime"], False),
    ]
    d.add(f'<g class="{d.show(0.3)}" filter="url(#soft)"><rect x="130" y="160" width="1020" height="380" rx="16" fill="{C["term"]}" stroke="{C["blue2"]}" stroke-width="3"/>'
          f'<circle cx="156" cy="184" r="7" fill="#ef4444"/><circle cx="180" cy="184" r="7" fill="#f59e0b"/><circle cx="204" cy="184" r="7" fill="#22c55e"/>'
          f'<text x="640" y="190" fill="#64748b" font-size="14" text-anchor="middle">main.go</text></g>')
    n = 0
    for i, (s, col, key) in enumerate(lines):
        if not s:
            continue
        k = d.show(0.8 + n * 0.55, None, fade=0.25)
        n += 1
        y = 236 + i * 32
        if key:
            d.add(f'<rect class="{k}" x="146" y="{y-22}" width="6" height="28" rx="3" fill="{C["orange"]}"/>')
        d.add(f'<text class="mono {k}" x="166" y="{y}" fill="{col}" font-size="18" xml:space="preserve">{escape(s)}</text>')
    caption(d, "Wrap your handler, set it as the default logger, wrap your router.", 4.0, dur - 0.6, y=610)


def proof(d, dur):
    background(d, "Tested Like Production", FACTS.get("proof_sub", "Measured, not promised"), bg=False)
    tiles = FACTS.get("tiles", [])
    cols = [C["green"], C["blue"], C["purple"], C["orange"], C["teal"], C["red"]]
    for i, (big, small) in enumerate(tiles[:6]):
        x = 110 + (i % 3) * 360
        y = 170 + (i // 3) * 220
        k = d.pop(0.4 + i * 0.5)
        d.add(f'<g class="{k}" filter="url(#soft)"><rect x="{x}" y="{y}" width="330" height="190" rx="16" fill="{C["panel"]}" stroke="{cols[i]}" stroke-width="5"/></g>')
        d.add(f'<g class="{k}">')
        text(d, x + 165, y + 92, big, 44, cols[i], weight="800")
        for j, part in enumerate(small.split("\n")):
            text(d, x + 165, y + 132 + j * 22, part, 16, C["ink"], weight="600")
        d.add("</g>")


def outro(d, dur):
    d.add(f'<g class="{d.pop(0.3)}">')
    text(d, 640, 250, "flightrec", 80, "#f8fafc", weight="800")
    d.add("</g>")
    d.add(f'<g class="{d.show(1.0)}" filter="url(#soft)"><rect x="300" y="310" width="680" height="70" rx="14" fill="{C["term"]}" stroke="{C["green"]}" stroke-width="3"/></g>')
    text(d, 640, 354, "go get github.com/haawklabs/flightrec", 24, C["lime"], weight="700", cls=d.show(1.2), mono=True)
    text(d, 640, 440, "github.com/haawklabs/flightrec", 24, "#e2e8f0", weight="600", cls=d.show(2.0))
    text(d, 640, 480, "Apache-2.0 · Go 1.26+ · journal on Linux, macOS and the BSDs", 18, "#94a3b8", cls=d.show(2.4))


SCENES = [
    (7, title_card),
    (12, problem),
    (15, lambda d, dur: diagrams.how_it_works(14.0, d)),
    (9, adopt),
    (15, lambda d, dur: diagrams.cross_service(14.0, d)),
    (16, lambda d, dur: diagrams.crash_recovery(15.0, d)),
    (13, lambda d, dur: diagrams.otel(12.0, d)),
    (12, proof),
    (7, outro),
]


def build():
    total = sum(s for s, _ in SCENES)
    d = Doc("flightrec walkthrough", "What flightrec solves and how: per-request debug log buffering, triggers, "
            "cross-service flush, crash recovery and OpenTelemetry spans.", loop=float(total))
    d.still = 7 + 12 + 10.6  # the failed request in "How flightrec works"
    defs_common(d)
    background(d)
    t = 0.0
    for dur, fn in SCENES:
        scene(d, t, t + dur, fn)
        t += dur
    # progress bar
    k = d.keyframes([(0, "transform:scaleX(0)"), (total, "transform:scaleX(1)")])
    d.css.append(f".{k}{{transform-origin:0 0}}")
    d.add(f'<rect x="0" y="{H-4}" width="{W}" height="4" fill="#1e293b"/><rect class="{k}" x="0" y="{H-4}" width="{W}" height="4" fill="{C["orange"]}"/>')
    return d, total


if __name__ == "__main__":
    d, total = build()
    open(sys.argv[1], "w").write(d.svg())
    print(f"wrote {sys.argv[1]} ({total:.0f}s)")
