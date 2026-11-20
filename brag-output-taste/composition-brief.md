# Hyperframes Composition Brief: DeltaSync — "Holy Sync Engine" (taste re-skin)

## Objective
Recreate the DeltaSync brag video under the `taste` skill's direction: an angelcore × cloud-trance edit of a peer-to-peer file sync engine. Deadpan-serious. Same real product content (dashboard, telemetry, conflict row), re-graded to the taste palette and cut to the music's beat grid.

## Output
- Composition directory: `C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\brag-output-taste\composition\`
- Rendered video: `C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\brag-output-taste\brag.mp4`
- Format: landscape — 1920x1080
- Duration: 19.5 seconds

## Source Material
- Project root: `C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync`
- Primary files read: `README.md`, `src/DeltaSync.Cli/TerminalDashboard.cs`
- Product name: DeltaSync
- Tagline / strongest claim: peer-to-peer sync that moves only changed chunks; chaos-tested crash safety; vector-clock conflict resolution
- Key UI or visual moment to recreate: the `TerminalDashboard` (header banner, Engine Status panel, Telemetry & Performance panel, Recent Sync Events table) — restyled to the taste palette
- Copy that must appear verbatim:
  - `deltasync sync ./my-folder`
  - "peer-to-peer · no cloud · no server"
  - "DeltaSync" / "Peer-to-Peer Synchronization Engine"
  - "Engine Status" / "Telemetry & Performance" / "Recent Sync Events"
  - "State: SYNCING" / "Connected Peers: 3 active"
  - "Transferred (In/Out)" / "FastCDC Dedup Saved" / "Bandwidth Savings" / "Conflicts Resolved" / "Sync Latency (p95)"
  - "CONFLICT main.rs:142" / "vector clock divergence (ADR-0001)" / "RESOLVED"
  - "Only the delta moves."
  - "Two machines edited the same line. It chose."
  - "TLS 1.3" / "Merkle tree" / "FastCDC" / "Chaos-tested"
  - "Delta in. Conflict out."
  - `deltasync sync ./my-folder --port 4242`

## Creative Direction
- Tone preset: `polished` × taste (angelcore × cloud-trance)
- Creative direction: holy sync engine — a dev-infra film in the ethereal/divine family with a hyperpop accent
- Interpretation: weightless, holy, precise. Motion breathes; bursts only at locked cues. Hard cuts on the beat grid; no dissolves in tempo sections. One accent per shot: gold in the dark void, iridescence only in bright flash frames, red exactly once.
- Angle: the first brag was a clean terminal film; this is the same product re-graded as an ascension ritual — chunks glow like embers, only the delta ascends, a conflict flashes red and is resolved in gold.
- Hook: ember bloom in the void; the sync command types itself in bone white.
- Outro / punchline: "Delta in. Conflict out." over a gold bloom, crystalline dissolve after.
- Avoid:
  - Generic SaaS language
  - More than one accent color per shot
  - Dissolves in tempo sections
  - Shots held past their musical phrase
  - Bouncy eases, particles that aren't rising

## Visual Identity (taste palette)
- Background (void): `#05060a`
- Text (bone): `#f4f1ea`; dim bone: `#A8A29A`
- Divine accent: gold `#ffb24d`, ember `#ff7a18`
- Crystalline (bright flashes only): violet `#8a6bff`, cyan `#4fc3ff`, magenta `#ff6ad5`
- Danger (one shock cut): red `#ff2a2a`
- Panel surface: `#0C0E13`, border `#26221C`
- Display/body font: local monospace stack (Cascadia Mono / Consolas / JetBrains Mono via @font-face src:local)
- Visual references: hero-on-black monolith chunk grid; drifting bokeh; rising embers; radial-gradient divine blooms with mix-blend-mode: screen

## Storyboard
Creative contract: `C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\brag-output-taste\brag-plan.md` (full beat map + per-scene storyboard).

Scene summary (beat grid 0.5457 s/beat @ 109.96 BPM):
1. Hook: the command — 2.73s (beats 0–5) — ember bloom; `deltasync sync ./my-folder` types char-by-char; dim sub-line.
2. Dashboard reveal — 4.37s (beats 5–13) — real dashboard in monochrome bone/dim; `SYNCING` punches gold at beat 9; telemetry counters count up; event rows land on beats 9/10/11.
3a. Hero macro — 1.65s (beats 13–16) — 8 monolith slabs centered in pure black; slabs 2 & 5 glow gold; slow float.
3b. The delta, close — 1.09s (beats 16–18) — hard cut (strong cue 8.74) to close-up of the 2 gold slabs, scale punch.
3c. Ascension — 2.19s (beats 18–22) — dim label + 2 gold ember-orbs accelerate across a faint bus; caption beat-grid snapped to beat 19 (10.37s; readability floor outranks the 10.93 strong cue): "Only the delta moves."
4. Conflict, resolved — 3.27s (beats 22–28) — red flash + glitch; CONFLICT row in red; RESOLVED stamps gold on strong cue 13.11 with iridescent divine flash; caption at beat 25.
5. Ascension + punchline — 3.82s (beats 28–35) — chips on beats 28–31; final line on strong cue 17.47 with gold bloom; crystalline dissolve on strong cue 18.56; fade to black.

## Audio
- Audio role: steady ethereal-adjacent bed with sparse ritualistic accents
- Audio arc: mechanical key ticks → dashboard hum → red glitch stab → ding → final bell → fade
- Music: `assets/music/happy-beats-business-moves-vol-12-by-ende-dot-app.mp3` (copied into the composition)
- Music treatment: volume ~0.32, start 0s, hold under the final line, fade out over the last ~0.8s
- Music cue guidance: bundled preset at `assets/music/happy-beats-business-moves-vol-12-by-ende-dot-app.music-cues.json` (~109.96 BPM, 0.5457 s/beat). Strong-cue locks: 8.74 (delta close-up punch), 13.11 (RESOLVED stamp), 17.47 (final line), 18.56 (crystalline dissolve). Chip reveals snap to beats 15.29 / 15.84 / 16.38 / 16.93. Caption is beat-grid snapped (beat 19, 10.37s) — the 10.93 cue left it under the readability floor.
- Audio-reactive treatment: skipped (documented) — embers and bokeh already carry the breathing motion; extraction would add complexity without serving the story.
- Audio-coupled moments:
  - typed command — randomized `assets/sfx/keyboard/keypress-*.wav`
  - dashboard landing — `assets/sfx/interface/drop_001.ogg`
  - packet flight — 2 × `assets/sfx/interface/click_001.ogg`
  - conflict shock — `assets/sfx/interface/glitch_002.ogg`
  - RESOLVED stamp — `assets/sfx/casino/chips-collide-1.ogg`
  - final line — `assets/sfx/impact/impactBell_heavy_000.ogg`
- SFX selection guidance: polished posture — motion-matched, whisper-quiet except the final bell; ≤6 SFX families
- SFX analysis guidance: `C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\.agents\skills\brag\assets\sfx\sfx-analysis.md`
- Exact SFX choice: choose timestamps/density/volume based on the implemented animation.
- Audio files: music + SFX already copied into `<composition>/assets/`; all audio paths relative to the composition directory.

## Hyperframes Instructions
Load the composition-building Hyperframes domain skills — `hyperframes-core`, `hyperframes-animation`, `hyperframes-creative`, `hyperframes-keyframes`, `hyperframes-cli`. /brag is its own workflow; do not enter the `hyperframes` entry-point intent interview.

Requirements:
- Show at least one real UI/copy/visual element from the source project (the recreated dashboard + event feed satisfy this).
- Keep all text readable; keep the video within 15–25 seconds (19.5s).
- Include the planned music/SFX layer.
- Treat `/brag` audio notes and the taste beat map as guidance, not a fixed cue sheet; choose exact SFX after the animation exists.
- Major reveals may move toward nearby strong cues within ~0.15s; smaller entrances within ~0.10s. 1–5 strong-cue locks are intentional here (the taste grammar is beat-locked cuts).
- Honor the planned music treatment (final bell rings over the bed; fade-out at the end).
- Determinism: seeded PRNG only (no unseeded Math.random), finite repeats, no render-time clocks.
- Run `npx hyperframes check` before render — it is brag's single gate.
