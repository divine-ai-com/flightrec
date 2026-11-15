"""Generates flightrec's animated architecture diagrams as SVG files."""

import sys
from kit import *  # noqa: F401,F403


# Frames shown when the viewer prefers reduced motion.
STILLS = {"How flightrec works": 10.6, "Cross-service flush": 10.0, "Crash recovery": 11.5, "OpenTelemetry spans": 8.0}


def start(d, title, desc, loop, heading, subheading):
    """Creates a standalone diagram, or draws into a composed walkthrough."""
    if d is None:
        d = Doc(title, desc, loop=loop)
        d.still = STILLS.get(title, loop * 0.75)
        defs_common(d)
        background(d, heading, subheading)
    else:
        background(d, heading, subheading, bg=False)
    return d


def how_it_works(loop=14.0, d=None):
    """Healthy request: buffered then dropped, one summary line out.
    Failed request: every buffered line written, in order."""
    d = start(d, "How flightrec works",
              "A healthy request's debug logs are buffered and dropped, leaving one summary line; "
              "a failed request writes every buffered debug line in order.", loop,
              "How flightrec Works", "Debug logs are kept per request and written only when the request fails")

    # client
    icon_user(d, 110, 330)
    pill(d, 110, 420, "Client", C["blue2"], size=17)
    text(d, 110, 455, "HTTP / gRPC requests", 14, "#94a3b8")

    # service panel
    panel(d, 250, 160, 520, 400, C["orange"], "Your service")
    pill(d, 380, 215, "HTTPMiddleware", C["blue"], size=14)
    pill(d, 600, 215, "grpcflightrec", C["blue"], size=14)
    d.add(f'<rect x="290" y="250" width="440" height="56" rx="10" fill="{C["term"]}"/>')
    text(d, 306, 284, 'slog.DebugContext(ctx, "charging card", ...)', 15, "#a5f3fc", anchor="start", mono=True)

    # recorder box with buffer
    d.add(f'<rect x="290" y="326" width="440" height="210" rx="12" fill="{C["panel2"]}" stroke="{C["blue"]}" stroke-width="2.5"/>')
    text(d, 510, 354, "flightrec Recorder: one buffer per request", 16, C["ink"], weight="700")
    icon_gear(d, 330, 440, 22, C["green"])
    text(d, 330, 492, "scope", 13, C["muted"])

    # buffer slots
    bx, by = 380, 376
    d.add(f'<rect x="{bx}" y="{by}" width="330" height="134" rx="8" fill="#fff" stroke="#cbd5e1" stroke-width="2" stroke-dasharray="6 5"/>')
    lines_a = [(1.7, 250), (2.1, 200), (2.5, 280), (2.9, 170)]
    for i, (t0, w) in enumerate(lines_a):
        k = d.keyframes([(0, "opacity:0"), (t0, "opacity:0;transform:translateX(-14px)"), (t0 + .3, "opacity:1;transform:translateX(0)"),
                         (4.0, "opacity:1"), (4.6, "opacity:0;transform:translateY(10px)"), (loop, "opacity:0")])
        log_line(d, bx + 14, by + 14 + i * 24, w, "#94a3b8", cls=k)
    lines_b = [(8.2, 240), (8.6, 190), (9.0, 270), (9.4, 210)]
    for i, (t0, w) in enumerate(lines_b):
        k = d.keyframes([(0, "opacity:0"), (t0, "opacity:0;transform:translateX(-14px)"), (t0 + .3, "opacity:1;transform:translateX(0)"),
                         (10.4 + i * .15, "opacity:1;transform:translateX(0)"), (10.9 + i * .15, "opacity:0;transform:translateX(120px)"), (loop, "opacity:0")])
        log_line(d, bx + 14, by + 14 + i * 24, w, C["sky"], cls=k)
    kerr = d.keyframes([(0, "opacity:0"), (9.8, "opacity:0"), (10.0, "opacity:1"), (11.2, "opacity:1"), (11.6, "opacity:0"), (loop, "opacity:0")])
    d.add(f'<g class="{kerr}"><rect x="{bx+14}" y="{by+14+4*24}" width="300" height="18" rx="6" fill="{C["red"]}"/>'
          f'<text class="mono" x="{bx+24}" y="{by+14+4*24+14}" fill="#fff" font-size="13" font-weight="700">ERROR payment declined</text></g>')
    # buffer label
    text(d, bx + 165, by + 152, "in memory, bounded per request and per process", 12, C["muted"])

    # status chips
    kok = d.pop(3.3, 6.2)
    pill(d, 510, 600, "200 OK: healthy", C["green"], cls=kok, size=17)
    kfail = d.pop(10.0, 13.2)
    pill(d, 510, 600, "Error log: trigger", C["red"], cls=kfail, size=17)

    # outcome panels
    kg = d.keyframes([(0, "opacity:.6"), (3.5, "opacity:.6"), (3.9, "opacity:1"), (6.4, "opacity:1"), (6.8, "opacity:.6"), (loop, "opacity:.6")])
    panel(d, 850, 170, 380, 150, C["green"], "Healthy request", C["green"], cls=kg)
    d.add(f'<g class="{kg}">')
    text(d, 1040, 236, "debug lines dropped", 18, C["ink"], weight="700")
    text(d, 1040, 266, "1 summary line written", 16, C["muted"])
    log_line(d, 935, 284, 210, C["lime"])
    d.add("</g>")
    kr = d.keyframes([(0, "opacity:.6"), (10.0, "opacity:.6"), (10.4, "opacity:1"), (13.3, "opacity:1"), (13.7, "opacity:.6"), (loop, "opacity:.6")])
    panel(d, 850, 380, 380, 180, C["red"], "Failed request", C["red"], cls=kr)
    d.add(f'<g class="{kr}">')
    text(d, 1040, 446, "every buffered line written", 18, C["ink"], weight="700")
    text(d, 1040, 474, "in order, with the trace ID and reason", 15, C["muted"])
    for i, w in enumerate([230, 180, 260, 200]):
        log_line(d, 925, 492 + i * 14, w, C["sky"], h=9)
    d.add("</g>")

    # arrows
    arrow(d, "M170 330 C210 330 220 330 246 330", "#e2e8f0", "arrw")
    arrow(d, "M772 300 C810 300 810 245 846 245", C["lime"], "arrg")
    arrow(d, "M772 460 C810 460 810 470 846 470", C["red"], "arrr")

    mover(d, "M120 300 C190 300 220 330 300 330", 0.6, 0.9, C["lime"], label="GET /orders")
    mover(d, "M120 300 C190 300 220 330 300 330", 7.0, 0.9, C["orange"], label="POST /checkout")
    mover(d, "M730 300 C810 300 810 245 930 245", 4.2, 0.8, C["lime"], label="1 line")
    for i in range(4):
        mover(d, "M730 460 C810 460 810 470 930 470", 10.6 + i * .18, 0.8, C["sky"], r=6)

    # triggers row
    text(d, 640, 650, "Triggers", 15, "#94a3b8", weight="700")
    trig = ["Error log", "5xx / gRPC error", "Panic", "Slow request", "Flush(ctx)", "Downstream / remote"]
    xs = [215, 380, 520, 650, 790, 960]
    for x, s in zip(xs, trig):
        pill(d, x, 690, s, "#1e293b", size=14, color="#e2e8f0")
    return d


def cross_service(loop=14.0, d=None):
    d = start(d, "Cross-service flush",
            "When inventory fails, the Flightrec-Flush response header travels back up to orders and gateway, and a "
            "notifier (Redis or NATS) tells services that already finished, so every service keeps its logs for the request.", loop,
              "Keep Logs Across Services", "A failure deep in the call chain keeps the logs of every service that handled the request")

    xs = [220, 640, 1060]
    names = ["gateway", "orders", "inventory"]
    for i, (x, n) in enumerate(zip(xs, names)):
        border = C["red"] if n == "inventory" else C["blue"]
        panel(d, x - 150, 170, 300, 230, border, n, C["red"] if n == "inventory" else C["blue2"])
        icon_server(d, x - 80, 260, 0.9, border)
        icon_gear(d, x + 70, 255, 20, C["green"])
        text(d, x, 330, "flightrec Recorder", 15, C["ink"], weight="700")
        for j in range(3):
            log_line(d, x - 110, 346 + j * 14, 220 - j * 40, C["sky"] if n != "inventory" else "#fca5a5", h=9)

    # forward calls
    arrow(d, f"M{xs[0]+152} 220 L{xs[1]-156} 220", "#e2e8f0", "arrw")
    arrow(d, f"M{xs[1]+152} 220 L{xs[2]-156} 220", "#e2e8f0", "arrw")
    text(d, 430, 206, "call (trace ID)", 13, "#94a3b8")
    text(d, 850, 206, "call (trace ID)", 13, "#94a3b8")
    mover(d, f"M{xs[0]+152} 220 L{xs[1]-156} 220", 0.5, 0.8, "#e2e8f0", r=6)
    mover(d, f"M{xs[1]+152} 220 L{xs[2]-156} 220", 1.4, 0.8, "#e2e8f0", r=6)

    # failure
    kx = d.pop(2.4, 13.4)
    d.add(f'<g class="{kx}">')
    icon_bolt(d, xs[2] + 120, 160, 0.9)
    d.add("</g>")
    pill(d, xs[2], 440, "503: logs kept", C["red"], cls=d.pop(2.6, 13.4), size=15)

    # header bubbling back (synchronous)
    arrow(d, f"M{xs[2]-156} 290 L{xs[1]+156} 290", C["orange"], "arr")
    arrow(d, f"M{xs[1]-156} 290 L{xs[0]+156} 290", C["orange"], "arr")
    for x in (430, 850):
        text(d, x, 316, "Flightrec-Flush:", 12, C["orange"], weight="700", mono=True)
        text(d, x, 332, "http_503", 12, C["orange"], weight="700", mono=True)
    mover(d, f"M{xs[2]-156} 290 L{xs[1]+156} 290", 3.0, 0.9, C["orange"], r=7, label="header")
    pill(d, xs[1], 440, "downstream:http_503", C["orange"], cls=d.pop(4.0, 13.4), size=15)
    mover(d, f"M{xs[1]-156} 290 L{xs[0]+156} 290", 4.3, 0.9, C["orange"], r=7, label="header")
    pill(d, xs[0], 440, "downstream:http_503", C["orange"], cls=d.pop(5.3, 13.4), size=15)

    # notifier (asynchronous)
    panel(d, 340, 520, 600, 120, C["purple"], "Notifier: Redis or NATS", C["purple"])
    icon_db(d, 420, 586, 0.7, C["purple"])
    text(d, 680, 576, "async calls and callers that already finished:", 15, C["ink"], weight="700")
    text(d, 680, 602, "Options.Retain keeps their buffers; the notice flushes them", 14, C["muted"])
    arrow(d, f"M{xs[2]} 470 C{xs[2]} 560 1000 580 944 580", C["violet"], "arr")
    mover(d, f"M{xs[2]} 470 C{xs[2]} 560 1000 580 944 580", 6.6, 1.0, C["violet"], r=7, label="publish")
    arrow(d, f"M336 580 C{xs[0]} 580 {xs[0]} 520 {xs[0]} 474", C["violet"], "arr")
    mover(d, f"M336 580 C{xs[0]} 580 {xs[0]} 520 {xs[0]} 474", 7.9, 1.0, C["violet"], r=7, label="remote")
    text(d, 640, 680, "Retained buffers flush with reason remote:<reason>; every notice is rate limited and validated", 14, "#94a3b8")
    return d


def crash_recovery(loop=15.0, d=None):
    d = start(d, "Crash recovery",
            "Buffered records are also written to a memory-mapped journal file. After kill -9, the next start "
            "recovers the requests that were in flight and writes their debug logs, tagged crash_recovery.", loop,
              "Survive a Hard Kill", "kill -9, out-of-memory kills and expired grace periods no longer lose in-flight requests")

    # process panel
    kproc = d.keyframes([(0, "opacity:1"), (5.0, "opacity:1"), (5.3, "opacity:.25"), (7.6, "opacity:.25"), (7.9, "opacity:1"), (loop, "opacity:1")])
    panel(d, 90, 180, 420, 360, C["orange"], "checkout service", cls=kproc)
    d.add(f'<g class="{kproc}">')
    text(d, 300, 236, "requests in flight", 16, C["ink"], weight="700")
    for i, (lbl, col) in enumerate([("GET /cart", C["green"]), ("POST /checkout", C["amber"]), ("POST /refund", C["amber"])]):
        y = 262 + i * 56
        d.add(f'<rect x="130" y="{y}" width="340" height="44" rx="10" fill="#fff" stroke="#cbd5e1" stroke-width="2"/>')
        text(d, 146, y + 28, lbl, 15, C["ink"], anchor="start", weight="600", mono=True)
        for j in range(3):
            k = d.keyframes([(0, "opacity:0"), (0.6 + i * .4 + j * .5, "opacity:0"), (0.9 + i * .4 + j * .5, "opacity:1"), (loop, "opacity:1")])
            log_line(d, 318 + j * 46, y + 16, 38, col, cls=k, h=12)
    text(d, 300, 450, "every buffered record is also copied", 14, C["muted"])
    text(d, 300, 470, "into a shared memory mapping", 14, C["muted"])
    d.add("</g>")

    # journal
    panel(d, 600, 200, 280, 320, C["amber"], "journal file", C["amber"], fill="#fffbeb")
    icon_file(d, 740, 300, 1.1, C["amber"])
    text(d, 740, 390, "mmap ring, checksummed", 15, C["ink"], weight="700")
    text(d, 740, 414, "kept by the kernel when", 14, C["muted"])
    text(d, 740, 434, "the process dies", 14, C["muted"])
    pill(d, 740, 480, "emptyDir volume", C["amber"], size=14)
    arrow(d, "M512 340 L596 340", C["amber"], "arr")
    for i in range(5):
        mover(d, "M470 300 C540 300 560 340 650 340", 0.8 + i * .7, 0.7, C["amber"], r=5)

    # kill
    kk = d.pop(4.8, 7.8)
    d.add(f'<g class="{kk}">')
    icon_bolt(d, 300, 360, 2.0)
    d.add(f'<rect x="170" y="560" width="260" height="44" rx="10" fill="{C["red"]}"/>')
    text(d, 300, 589, "kill -9  (no defer runs)", 18, "#fff", weight="700", mono=True)
    d.add("</g>")
    krs = d.pop(8.0, 14.6)
    pill(d, 300, 582, "restart: StartJournal(path)", C["blue2"], cls=krs, size=16)

    # recovery output
    arrow(d, "M882 360 L940 360", C["lime"], "arrg")
    mover(d, "M880 360 L1000 360", 8.6, 0.6, C["lime"], r=7, label="recover")
    term = [
        ('{"level":"DEBUG","msg":"loaded cart",', "#cbd5e1"),
        (' "flightrec.flush_reason":"crash_recovery",', C["lime"]),
        (' "flightrec.recovered":true, ...}', C["lime"]),
        ('{"level":"DEBUG","msg":"charging card", ...}', "#cbd5e1"),
        ('{"level":"ERROR",', "#fca5a5"),
        (' "msg":"request interrupted by crash",', "#fca5a5"),
        (' "method":"POST","path":"/checkout", ...}', "#fca5a5"),
    ]
    kt = d.show(8.8, 14.6)
    terminal(d, 940, 200, 320, 320, [], cls=kt, title="next start: logs")
    for i, (s, col) in enumerate(term):
        k = d.show(9.2 + i * .45, 14.6, fade=.2)
        d.add(f'<text class="mono {k}" x="954" y="{252 + i*22}" fill="{col}" font-size="11.5" xml:space="preserve">{escape(s)}</text>')
    text(d, 640, 650, "Torn writes skipped by checksum  ·  a crash during recovery is retried 3 times  ·  stop() zeros the file", 15, "#94a3b8")
    return d


def otel(loop=12.0, d=None):
    d = start(d, "OpenTelemetry spans",
            "otelflightrec buffers a trace's child spans and exports only the root span when the trace is healthy, "
            "and every span when it fails.", loop,
              "Traces: Export What Matters", "otelflightrec.SpanProcessor sits in front of your exporter")

    # span tree
    panel(d, 80, 170, 330, 400, C["blue"], "Trace: 1 root + 10 spans")
    d.add(f'<rect x="110" y="214" width="270" height="22" rx="6" fill="{C["blue"]}"/>')
    text(d, 120, 230, "HTTP POST /checkout", 13, "#fff", anchor="start", weight="700")
    for i in range(10):
        col = C["red"] if i == 9 else C["sky"]
        k = d.keyframes([(0, "opacity:1"), (loop, "opacity:1")]) if i != 9 else d.keyframes([(0, f"fill:{C['sky']}"), (6.6, f"fill:{C['sky']}"), (6.8, f"fill:{C['red']}"), (loop, f"fill:{C['red']}")])
        d.add(f'<rect class="{k}" x="{130 + (i%3)*10}" y="{246 + i*30}" width="{200 - (i%3)*10}" height="18" rx="5" fill="{col}"/>')
    text(d, 245, 560, "db.query, cache.get, ...", 13, C["muted"])

    panel(d, 480, 220, 320, 300, C["green"], "SpanProcessor", C["green"])
    icon_gear(d, 560, 330, 30, C["green"])
    text(d, 700, 320, "buffers child spans", 15, C["ink"], weight="700")
    text(d, 700, 344, "per trace", 15, C["ink"], weight="700")
    text(d, 640, 420, "healthy: root only", 16, C["green"], weight="700")
    text(d, 640, 450, "failed: every span", 16, C["red"], weight="700")
    text(d, 640, 490, "linked to the same trace as the logs", 13, C["muted"])

    panel(d, 880, 220, 320, 300, C["purple"], "OTLP exporter", C["purple"])
    icon_db(d, 1040, 330, 0.9)
    kh = d.show(1.2, 5.6)
    kf = d.show(7.2, 11.6)
    text(d, 1040, 430, "1 span · 110 B", 22, C["green"], weight="700", cls=kh)
    text(d, 1040, 460, "56% less CPU than exporting all", 14, C["muted"], cls=kh)
    text(d, 1040, 430, "11 spans · 2.4 KB", 22, C["red"], weight="700", cls=kf)
    text(d, 1040, 460, "the whole failed trace", 14, C["muted"], cls=kf)

    arrow(d, "M412 370 L476 370", "#e2e8f0", "arrw")
    arrow(d, "M802 370 L876 370", C["orange"], "arr")
    for i in range(6):
        mover(d, "M380 370 L520 370", 0.4 + i * .15, 0.5, C["sky"], r=5)
        mover(d, "M380 370 L520 370", 6.2 + i * .15, 0.5, C["sky"] if i < 5 else C["red"], r=5)
    mover(d, "M760 370 L960 370", 1.6, 0.7, C["blue"], r=7, label="root")
    for i in range(6):
        mover(d, "M760 370 L960 370", 7.4 + i * .14, 0.7, C["red"] if i == 5 else C["sky"], r=6)
    text(d, 640, 640, "Measured with BatchSpanProcessor + OTLP/HTTP on a GitHub runner: healthy trace 34.9 µs CPU vs 79.5 µs", 15, "#94a3b8")
    return d


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "."
    for name, fn in [("how-it-works", how_it_works), ("cross-service", cross_service), ("crash-recovery", crash_recovery), ("otel", otel)]:
        with open(f"{out}/{name}.svg", "w") as f:
            f.write(fn().svg())
        print("wrote", name)
