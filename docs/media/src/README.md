# Diagram and walkthrough sources

The SVGs in `docs/media` are generated; edit these files, not the SVGs.

- `kit.py`: shared style (dark grid, panels, pills, icons, flowing arrows). Animations are CSS only, so they play inside an `<img>` on GitHub.
- `diagrams.py`: the four architecture diagrams.
- `walkthrough.py`: the walkthrough, composed of scenes and the diagrams. `facts.json` holds the numbers shown in its "Tested Like Production" scene; each comes from the README's Performance section or from [docs/e2e-report.md](../../e2e-report.md).
- `render.mjs`: renders an SVG to a VP9 `.webm` by stepping its animations frame by frame in headless Chromium, or writes stills (`stills`) and the reduced-motion frame (`reduced`). It needs Node 22 or later (built-in `fetch` and `WebSocket`) and Playwright's Chromium and ffmpeg builds in `~/.cache/ms-playwright` (`npx playwright install chromium ffmpeg`), and no npm packages.

```bash
cd docs/media/src
python3 diagrams.py ..
python3 walkthrough.py ../walkthrough.svg facts.json
node render.mjs ../walkthrough.svg video ../walkthrough.webm 24 106
```
