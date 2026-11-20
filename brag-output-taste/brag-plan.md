# Brag Plan: DeltaSync — "Holy Sync Engine" (taste re-skin)

## Taste decision (made first, per the taste skill)

- **Primary family:** Ethereal / divine
- **Accent family:** Hyperpop / Y2K-cyber
- **Genre name:** angelcore × cloud-trance dev-infra film
- **Premise:** a peer-to-peer file sync engine treated as an ascension ritual — deadpan-serious. Only changed chunks rise; conflicts are exorcised; the folder achieves sainthood. The humor is the juxtaposition: holy-trance film grammar applied to an extremely serious .NET sync engine.
- **Coherence rule (taste):** one accent per shot — gold lives in the dark void; iridescence appears only in bright flash frames; red appears exactly once (the CONFLICT shock cut).

## What is this app?

DeltaSync keeps folders identical across machines: delta transfer via FastCDC chunking, vector-clock conflict resolution, TLS 1.3 transport, chaos-tested crash safety. This video says it in the language of cloud-trance: chunks glow like embers, only the delta ascends, and a conflict flashes red before being resolved in gold.

## The angle

The first brag was a clean terminal product film. This one is the same product, re-graded as an angelcore × cloud-trance edit: near-black void, bone-white type, one molten-gold accent, rising embers, divine-bloom transitions, and hard cuts locked to the music's beat grid. Same claims, real dashboard, real numbers — different church.

## Hook (first 2.7s)

Void. A single ember bloom rises center-frame. Gold prompt, then the command types itself in bone white with randomized keypress ticks: `deltasync sync ./my-folder`. Dim bone line settles beneath: "peer-to-peer · no cloud · no server".

## Key moments (the middle)

- **Dashboard reveal (beat 5→13):** the real Spectre.Console dashboard materializes in monochrome bone/dim — then `SYNCING` punches gold on the downbeat (color-pop on monochrome). Telemetry counters count up: 2.41 MB / 1.87 MB, 812.3 KB/s, 1.94 MB dedup, 47.2% savings, 18.4 ms p95. Event feed: PEER joins, SYNC chunks.
- **Hero-on-black macro (beats 13→22):** the file as 8 glowing monolith slabs centered in pure black, slow float. Hard cut (beat 16, strong cue 8.74) to a close-up of the 2 gold delta slabs. Hard cut (beat 18) to packet flight — two gold embers accelerate across the void (speed-ramp feel, power2.in) — then the caption locks to strong cue 10.93: "Only the delta moves."
- **Conflict shock (beats 22→28):** red flash (the one allowed danger red), `CONFLICT main.rs:142 — vector clock divergence (ADR-0001)` in red on the void. One held breath. Strong cue 13.11: RESOLVED stamps in gold with an iridescent divine flash. Caption: "Two machines edited the same line. It chose."

## Outro / punchline

Beat 28→35: chips rise one per beat — `TLS 1.3` `Merkle tree` `FastCDC` `Chaos-tested`. Strong cue 17.47: the final line slams in, gold bloom behind it: **"Delta in. Conflict out."** Dim command beneath. Strong cue 18.56: iridescent crystalline bokeh rises and dissolves. Slow fade to black.

## Tone

- Preset: `polished` × taste (angelcore × cloud-trance)
- Interpretation: weightless, holy, precise. Motion breathes (slow drift, slow push); bursts only at locked cues. No bouncy eases. Every cut lands on the beat grid; no dissolves in tempo sections.

## Format: landscape — 1920x1080
## Duration: 19.5 seconds (35.7 beats @ 109.96 BPM, 0.5457 s/beat)

## Visual identity (taste palette)

- Background (void): `#05060a`
- Text (bone): `#f4f1ea`; dim bone `#A8A29A`
- Divine accent: gold `#ffb24d` → ember `#ff7a18` (the only warm light)
- Crystalline accent (bright flash frames only): violet `#8a6bff`, cyan `#4fc3ff`, magenta `#ff6ad5`
- Danger accent (one shock cut only): red `#ff2a2a`
- Panel surface: `#0C0E13` with warm dark border `#26221C`
- Display/body font: monospace stack (Cascadia Mono / Consolas / JetBrains Mono, local @font-face)
- Strongest visual element: the 8-chunk monolith grid floating in pure black (hero-on-black macro)

## Share copy (draft)

Introducing DeltaSync: peer-to-peer file sync that moves only the delta — not the whole file. FastCDC dedup, vector-clock conflict resolution, chaos-tested crash safety. Now with 100% more ascension.

## Audio direction

- Role: steady ethereal-adjacent bed with sparse, ritualistic accents
- Music: `happy-beats-business-moves-vol-12-by-ende-dot-app.mp3` (steady, clean; ~109.96 BPM), volume ~0.32, fade out over last ~0.8s
- Music cue guidance: bundled preset (`assets/music/happy-beats-business-moves-vol-12-by-ende-dot-app.music-cues.json`). Beat grid ≈ 0.5457s/beat. Strong-cue locks: 8.74 (delta close-up punch), 13.11 (RESOLVED stamp), 17.47 (final line), 18.56 (crystalline dissolve). Chip reveals snap to beats 28–31 (15.29 / 15.84 / 16.38 / 16.93). Caption is beat-grid snapped (beat 19, 10.37s) — the 10.93 cue left it under the 1.2s readability floor.
- Audio-reactive treatment: skipped (documented) — embers/bokeh carry the breath; extraction would add complexity without adding to the story.
- SFX posture: minimal but present; motion-matched; whisper-quiet except the final bell
  - typed command — randomized `keyboard/keypress-*.wav` per 3rd character
  - dashboard landing — `interface/drop_001.ogg` soft
  - packet flight — 2 × `interface/click_001.ogg`
  - conflict shock — `interface/glitch_002.ogg`
  - RESOLVED — `casino/chips-collide-1.ogg`
  - final line — `impact/impactBell_heavy_000.ogg` (the only louder moment)
- Restraint rule: ≤6 SFX families; nothing but the final bell may peak over the bed.

## Beat map (0.5457 s/beat, 30fps ≈ 16.37 frames/beat)

| Beats | Time | Cut |
|---|---|---|
| 0–5 | 0–2.73 | S1 hook: ember bloom, typed command |
| 5–13 | 2.73–7.09 | S2 dashboard: monochrome reveal, gold SYNCING punch (beat 9) |
| 13–16 | 7.09–8.74 | S3a hero macro: 8 monolith slabs |
| 16–18 | 8.74–9.83 | S3b delta close-up (strong cue lock) |
| 18–22 | 9.83–12.02 | S3c packet flight + label + caption (beat 19) |
| 22–28 | 12.02–15.29 | S4 conflict: red shock → RESOLVED (strong cue 13.11) |
| 28–35 | 15.29–19.5 | S5 chips → final line (17.47) → crystalline dissolve (18.56) |

## Storyboard

### Scene 1 — Hook: the command — 2.73s (beats 0–5)
Void. Ember bloom rises center. Gold `>` prompt, command types char-by-char (key ticks). Sub-line settles at beat 3. Cursor blinks.
Sequential/interaction: yes — per-character typing.
Audio intent: quiet, mechanical, reverent.
Audio-coupled idea: randomized keypress ticks.
Transition: divine gold bloom → Scene 2 (hard cut at beat 5).

### Scene 2 — Dashboard reveal — 4.37s (beats 5–13)
The real DeltaSync dashboard in monochrome bone/dim on void, with drifting bokeh behind. Header, Engine Status panel (Sync Path, State, Peers, Prometheus), Telemetry panel (counters count up 4.0–6.3s), Recent Sync Events (PEER/SYNC rows at beats 9/10/11). `SYNCING` punches gold at beat 9 (color-pop).
Sequential/interaction: yes — panels drop in sequence; counters tick; SYNCING punch.
Audio intent: the engine coming online, soft.
Audio-coupled idea: soft drop at dashboard landing; counter ticks implicit in music.
Transition: gold bloom → Scene 3a (hard cut at beat 13).

### Scene 3a — Hero macro: the file — 1.65s (beats 13–16)
Pure black. 8 monolith slabs (128px) centered in a row, hairline bone borders; slabs 2 and 5 glow gold (the delta). Slabs float slowly (±12px, sine). Embers rise through.
Sequential/interaction: yes — slabs settle in with 0.07s stagger, then breathe.
Audio intent: floating, held.
Transition: hard cut at beat 16 (strong cue 8.74) → Scene 3b.

### Scene 3b — The delta, close — 1.09s (beats 16–18)
Hard cut to a tight close-up of the 2 gold slabs (220px, glowing), scale punch on the cut, embers rising around them. The rest of the file is black negative space.
Sequential/interaction: yes — scale punch 8.74, settle by 9.3.
Audio intent: one warm accent.
Transition: hard cut at beat 18 → Scene 3c.

### Scene 3c — Ascension — 2.19s (beats 18–22)
Void with a faint bone bus line and a dim label ("main.rs · 2 of 8 chunks changed"). Two gold ember-orbs launch at beats 18 and 19, accelerating across (power2.in — the speed-ramp feel). Caption lands on beat 19 (10.37s, beat-grid snapped — readability floor outranks the 10.93 strong cue): "Only the delta moves." (bone, "delta" in gold), sub "FastCDC chunking · 47% less bandwidth", held 1.65s.
Sequential/interaction: yes — packet flight ×2, then caption.
Audio-coupled idea: 2 soft clicks at packet launch.
Transition: gold bloom → Scene 4 (hard cut at beat 22).

### Scene 4 — Conflict, resolved — 3.27s (beats 22–28)
Red flash + glitch SFX (the single danger-red moment). On the void: the event row `12:04:31.204 · CONFLICT · main.rs:142 — vector clock divergence (ADR-0001)`, red type, red glow breathing. Held tension for 1 beat. Strong cue 13.11: green→gold RESOLVED badge stamps over the row with an iridescent divine flash. Caption at beat 25: "Two machines edited the same line. It chose." sub "BGP-4 tie-breaking · RFC 4271 §6.8".
Sequential/interaction: yes — conflict row, tension hold, badge stamp, caption.
Audio-coupled idea: glitch on shock; chips-collide ding on the stamp.
Transition: gold bloom → Scene 5 (hard cut at beat 28).

### Scene 5 — Ascension + punchline — 3.82s (beats 28–35)
Chips rise one per beat (15.29 / 15.84 / 16.38 / 16.93): `TLS 1.3` `Merkle tree` `FastCDC` `Chaos-tested`. Strong cue 17.47: final line slams in — "Delta in. Conflict out." bone, "Delta" gold — over a gold bloom. Dim command beneath (beat 33). Strong cue 18.56: iridescent crystalline orbs rise and dissolve. Slow fade to black 19.05–19.45.
Sequential/interaction: yes — chips one-by-one (beat grid), then final line.
Audio-coupled idea: final bell on the punchline.
Transition: hold to end (poster frame ≈ 18.7s).

**Music mood for this video:** steady / ethereal-adjacent / confident
**Audio summary:** a low steady bed under a ritual: mechanical key ticks open it, the dashboard hums, one red glitch stabs the conflict, a ding exorcises it, and the final bell lands on "Delta in. Conflict out."
