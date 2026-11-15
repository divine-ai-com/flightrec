"""Shared drawing kit for flightrec's animated SVG diagrams.

Style: dark grid background, light rounded panels with thick glowing colored
borders, pill labels, simple flat icons, dashed flowing arrows and moving
dots. Animation is CSS only (keyframes, offset-path), so it plays inside an
<img> on GitHub and can be stepped frame by frame through the Web Animations
API when rendering video.
"""

from html import escape

W, H = 1280, 720
FONT = "Inter, 'Segoe UI', 'Helvetica Neue', Helvetica, Arial, 'DejaVu Sans', sans-serif"
MONO = "'JetBrains Mono', 'SFMono-Regular', Menlo, Consolas, 'DejaVu Sans Mono', monospace"

C = {
    "bg": "#090d16",
    "grid": "#172033",
    "panel": "#f3f7ff",
    "panel2": "#e7efff",
    "ink": "#0f172a",
    "muted": "#475569",
    "blue": "#2563eb",
    "blue2": "#1d4ed8",
    "sky": "#38bdf8",
    "orange": "#f97316",
    "red": "#ef4444",
    "green": "#16a34a",
    "lime": "#4ade80",
    "purple": "#7c3aed",
    "violet": "#a78bfa",
    "amber": "#f59e0b",
    "teal": "#14b8a6",
    "white": "#ffffff",
    "term": "#0b1220",
}


class Doc:
    def __init__(self, title, desc, w=W, h=H, loop=12.0):
        self.w, self.h, self.loop = w, h, loop
        self.title, self.desc = title, desc
        self.defs, self.css, self.body = [], [], []
        self._n = 0
        self.offset = 0.0  # added to every animation time, for composing scenes
        self.still = loop * 0.75  # frame shown when the viewer prefers reduced motion

    def uid(self, p="a"):
        self._n += 1
        return f"{p}{self._n}"

    # ---- animation ---------------------------------------------------------
    def keyframes(self, frames):
        """frames: list of (seconds, css) within the loop; returns a class name
        whose animation runs once per loop."""
        name = self.uid("k")
        steps = []
        for t, css in frames:
            t += self.offset
            pct = max(0.0, min(100.0, 100.0 * t / self.loop))
            steps.append(f"{pct:.3f}%{{{css}}}")
        self.css.append(f"@keyframes {name}{{{''.join(steps)}}}")
        self.css.append(f".{name}{{animation:{name} {self.loop}s linear infinite both}}")
        return name

    def show(self, t_in, t_out=None, fade=0.4, extra_in="", extra_out=""):
        """Opacity window: hidden, fades in at t_in, fades out at t_out."""
        fr = [(0, "opacity:0;" + extra_out)]
        if t_in > 0:
            fr.append((t_in, "opacity:0;" + extra_out))
        fr.append((t_in + fade, "opacity:1;" + extra_in))
        if t_out is not None:
            fr.append((t_out, "opacity:1;" + extra_in))
            fr.append((t_out + fade, "opacity:0;" + extra_out))
        fr.append((self.loop, ("opacity:0;" + extra_out) if t_out is not None else "opacity:1;" + extra_in))
        return self.keyframes(fr)

    def pop(self, t_in, t_out=None):
        k = self.show(t_in, t_out, extra_in="transform:scale(1)", extra_out="transform:scale(.85)")
        self.css.append(f".{k}{{transform-box:fill-box;transform-origin:center}}")
        return k

    # ---- output ------------------------------------------------------------
    def add(self, s):
        self.body.append(s)

    def svg(self):
        style = (
            f"text{{font-family:{FONT}}} .mono{{font-family:{MONO}}} "
            ".pop,.g{transform-box:fill-box;transform-origin:center} "
            f"@media (prefers-reduced-motion:reduce){{*{{animation-play-state:paused!important;animation-delay:-{self.still:.2f}s!important}}}}"
        )
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" height="{self.h}" role="img" aria-labelledby="t d">'
            f'<title id="t">{escape(self.title)}</title><desc id="d">{escape(self.desc)}</desc>'
            f"<style>{style}{''.join(self.css)}</style>"
            f"<defs>{''.join(self.defs)}</defs>"
            + "".join(self.body)
            + "</svg>"
        )


def defs_common(d):
    d.defs.append(
        f'<pattern id="grid" width="32" height="32" patternUnits="userSpaceOnUse">'
        f'<path d="M32 0H0V32" fill="none" stroke="{C["grid"]}" stroke-width="1"/></pattern>'
        '<filter id="glow" x="-150%" y="-150%" width="400%" height="400%"><feGaussianBlur stdDeviation="6" result="b"/>'
        '<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>'
        '<filter id="soft" x="-10%" y="-10%" width="120%" height="130%"><feDropShadow dx="0" dy="6" stdDeviation="8" flood-color="#000" flood-opacity=".45"/></filter>'
        f'<marker id="arr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{C["orange"]}"/></marker>'
        f'<marker id="arrw" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#e2e8f0"/></marker>'
        f'<marker id="arrg" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{C["lime"]}"/></marker>'
        f'<marker id="arrr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{C["red"]}"/></marker>'
    )
    d.css.append(
        "@keyframes flow{to{stroke-dashoffset:-36}} .flow{animation:flow .9s linear infinite}"
        "@keyframes pulse{0%,100%{opacity:.55}50%{opacity:1}} .pulse{animation:pulse 1.6s ease-in-out infinite}"
    )


def background(d, title=None, subtitle=None, bg=True):
    if bg:
        d.add(f'<rect width="{d.w}" height="{d.h}" fill="{C["bg"]}"/><rect width="{d.w}" height="{d.h}" fill="url(#grid)"/>')
    if title:
        tw = 34 * len(title) * 0.58 + 72
        x = (d.w - tw) / 2
        d.add(
            f'<rect x="{x:.0f}" y="22" width="{tw:.0f}" height="62" rx="6" fill="none" stroke="{C["red"]}" stroke-width="3" stroke-dasharray="10 7" class="flow"/>'
            f'<text x="{d.w/2}" y="64" fill="#f8fafc" font-size="34" font-weight="600" text-anchor="middle">{escape(title)}</text>'
        )
    if subtitle:
        d.add(f'<text x="{d.w/2}" y="110" fill="#94a3b8" font-size="18" text-anchor="middle">{escape(subtitle)}</text>')


def panel(d, x, y, w, h, border, label=None, label_fill=None, cls="", fill=None, sw=5):
    lf = label_fill or C["blue2"]
    out = [f'<g class="{cls}" filter="url(#soft)">']
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{fill or C["panel"]}" stroke="{border}" stroke-width="{sw}"/>')
    out.append("</g>")
    if label:
        lw = max(120, 11.5 * len(label) + 40)
        out.append(
            f'<g class="{cls}"><rect x="{x + w/2 - lw/2:.0f}" y="{y - 18}" width="{lw:.0f}" height="36" rx="18" fill="{lf}" stroke="#ffffff" stroke-opacity=".25"/>'
            f'<text x="{x + w/2:.0f}" y="{y + 6}" fill="#fff" font-size="18" font-weight="600" text-anchor="middle">{escape(label)}</text></g>'
        )
    d.add("".join(out))


def pill(d, x, y, text, fill, cls="", size=16, w=None, color="#fff"):
    pw = w or max(80, size * 0.62 * len(text) + 28)
    d.add(
        f'<g class="{cls}"><rect x="{x - pw/2:.0f}" y="{y - size}" width="{pw:.0f}" height="{size*2}" rx="{size}" fill="{fill}"/>'
        f'<text x="{x}" y="{y + size*0.36:.1f}" fill="{color}" font-size="{size}" font-weight="600" text-anchor="middle">{escape(text)}</text></g>'
    )


def text(d, x, y, s, size=16, fill=None, anchor="middle", weight="400", cls="", mono=False):
    f = ' class="mono ' + cls + '"' if mono else (f' class="{cls}"' if cls else "")
    d.add(f'<text{f} x="{x}" y="{y}" fill="{fill or C["muted"]}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}">{escape(s)}</text>')


def arrow(d, path, color=None, marker="arr", cls="", width=4, dashed=True):
    color = color or C["orange"]
    dash = ' stroke-dasharray="12 6"' if dashed else ""
    fl = " flow" if dashed else ""
    d.add(f'<path class="{cls}{fl}" d="{path}" fill="none" stroke="{color}" stroke-width="{width}" stroke-linecap="round"{dash} marker-end="url(#{marker})"/>')


def mover(d, path, t0, dur, color, r=8, label=None, hold_end=0.3):
    """A dot (optionally with a label chip) that travels along path once per
    loop, starting at t0 and taking dur seconds."""
    L = d.loop
    t0 += d.offset
    k = d.uid("m")
    p0 = 100 * t0 / L
    p1 = 100 * (t0 + dur) / L
    p2 = min(100, 100 * (t0 + dur + hold_end) / L)
    d.css.append(
        f"@keyframes {k}{{0%,{p0:.3f}%{{offset-distance:0%;opacity:0}}{p0+0.01:.3f}%{{opacity:1}}"
        f"{p1:.3f}%{{offset-distance:100%;opacity:1}}{p2:.3f}%,100%{{offset-distance:100%;opacity:0}}}}"
        f".{k}{{offset-path:path('{path}');offset-rotate:0deg;animation:{k} {L}s linear infinite both}}"
    )
    inner = f'<circle r="{r}" fill="{color}" filter="url(#glow)"/>'
    if label:
        lw = 9 * len(label) + 20
        inner += f'<rect x="{-lw/2:.0f}" y="{-r-30}" width="{lw:.0f}" height="22" rx="11" fill="{color}"/><text y="{-r-14}" fill="#fff" font-size="13" font-weight="700" text-anchor="middle">{escape(label)}</text>'
    d.add(f'<g class="{k}">{inner}</g>')


# ---- icons -----------------------------------------------------------------

def icon_server(d, x, y, s=1.0, color=None, cls=""):
    c = color or C["blue"]
    g = [f'<g class="{cls}" transform="translate({x} {y}) scale({s})">']
    for i in range(3):
        g.append(f'<rect x="-30" y="{-34 + i*24}" width="60" height="20" rx="4" fill="#1e293b" stroke="{c}" stroke-width="2"/>')
        g.append(f'<circle cx="-18" cy="{-24 + i*24}" r="3" fill="{C["lime"]}"/><rect x="-6" y="{-26 + i*24}" width="28" height="4" rx="2" fill="#64748b"/>')
    g.append("</g>")
    d.add("".join(g))


def icon_db(d, x, y, s=1.0, color=None, cls=""):
    c = color or C["purple"]
    d.add(
        f'<g class="{cls}" transform="translate({x} {y}) scale({s})">'
        f'<path d="M-34 -30v60c0 9 15 16 34 16s34-7 34-16v-60" fill="{c}"/>'
        f'<ellipse cx="0" cy="-30" rx="34" ry="14" fill="#c4b5fd"/>'
        f'<path d="M-34 -6c0 9 15 16 34 16s34-7 34-16M-34 14c0 9 15 16 34 16s34-7 34-16" fill="none" stroke="#c4b5fd" stroke-width="2.5"/></g>'
    )


def icon_gear(d, x, y, r=26, color=None, cls="", spin=True):
    c = color or C["green"]
    teeth = []
    import math
    for i in range(8):
        a = i * math.pi / 4
        teeth.append(f'<rect x="{-r*0.18:.1f}" y="{-r*1.25:.1f}" width="{r*0.36:.1f}" height="{r*0.5:.1f}" rx="2" fill="{c}" transform="rotate({math.degrees(a):.0f})"/>')
    k = ""
    if spin:
        k = d.uid("sp")
        d.css.append(f"@keyframes {k}{{to{{transform:rotate(360deg)}}}} .{k}{{transform-box:fill-box;transform-origin:center;animation:{k} 6s linear infinite}}")
    d.add(
        f'<g class="{cls}" transform="translate({x} {y})"><g class="{k}">' + "".join(teeth)
        + f'<circle r="{r}" fill="{c}"/><circle r="{r*0.42:.1f}" fill="{C["panel"]}"/></g></g>'
    )


def icon_user(d, x, y, cls=""):
    d.add(
        f'<g class="{cls}" transform="translate({x} {y})">'
        f'<circle cx="0" cy="-34" r="22" fill="#fcd9b6"/><path d="M-22 -40c2-18 42-18 44 0c-6-10-38-10-44 0z" fill="#1e293b"/>'
        f'<path d="M-38 30c0-30 16-40 38-40s38 10 38 40z" fill="{C["blue"]}"/>'
        f'<rect x="-46" y="8" width="70" height="44" rx="6" fill="#94a3b8"/><rect x="-42" y="12" width="62" height="36" rx="4" fill="#cbd5e1"/></g>'
    )


def icon_file(d, x, y, s=1.0, color=None, cls="", label=None):
    c = color or C["amber"]
    d.add(
        f'<g class="{cls}" transform="translate({x} {y}) scale({s})">'
        f'<path d="M-30 -40h40l20 20v60h-60z" fill="#fff" stroke="{c}" stroke-width="3"/><path d="M10 -40v20h20" fill="{C["panel2"]}" stroke="{c}" stroke-width="3"/>'
        + "".join(f'<rect x="-20" y="{-14 + i*12}" width="{36 - (i%2)*12}" height="5" rx="2" fill="{c}" opacity=".7"/>' for i in range(4))
        + (f'<text y="62" fill="#e2e8f0" font-size="14" text-anchor="middle" font-weight="600">{escape(label)}</text>' if label else "")
        + "</g>"
    )


def icon_bolt(d, x, y, s=1.0, cls=""):
    d.add(f'<g class="{cls}" transform="translate({x} {y}) scale({s})"><path d="M8 -44L-22 6h20l-8 38l32-52h-20z" fill="{C["amber"]}" stroke="#fff" stroke-width="2" filter="url(#glow)"/></g>')


def icon_cloud(d, x, y, cls=""):
    d.add(
        f'<g class="{cls}" transform="translate({x} {y})"><path d="M-46 22h92a24 24 0 0 0 0-48a34 34 0 0 0-64-8a26 26 0 0 0-28 56z" fill="{C["orange"]}"/>'
        f'<circle cx="0" cy="2" r="18" fill="none" stroke="#fff" stroke-width="3"/><path d="M-18 2h36M0 -16c-8 10-8 26 0 36M0 -16c8 10 8 26 0 36" fill="none" stroke="#fff" stroke-width="2.5"/></g>'
    )


def log_line(d, x, y, w, color, cls="", h=12, rx=6):
    d.add(f'<rect class="{cls}" x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{color}"/>')


def terminal(d, x, y, w, h, lines, cls="", title="terminal", t0=0.0, step=0.35, size=14):
    """A terminal window whose lines appear one by one from t0."""
    d.add(
        f'<g class="{cls}" filter="url(#soft)"><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{C["term"]}" stroke="{C["blue2"]}" stroke-width="3"/>'
        f'<circle cx="{x+20}" cy="{y+18}" r="6" fill="#ef4444"/><circle cx="{x+40}" cy="{y+18}" r="6" fill="#f59e0b"/><circle cx="{x+60}" cy="{y+18}" r="6" fill="#22c55e"/>'
        f'<text x="{x+w/2}" y="{y+23}" fill="#64748b" font-size="13" text-anchor="middle">{escape(title)}</text></g>'
    )
    for i, (s, col) in enumerate(lines):
        k = d.show(t0 + i * step, None, fade=0.15) if t0 is not None else ""
        d.add(f'<g class="{cls}"><text class="mono {k}" x="{x+18}" y="{y+52+i*(size+8)}" fill="{col}" font-size="{size}" xml:space="preserve">{escape(s)}</text></g>')
