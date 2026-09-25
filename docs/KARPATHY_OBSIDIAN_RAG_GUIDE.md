# The Karpathy Obsidian RAG + Claude Code Setup Guide

> Based on Andrej Karpathy's LLM Wiki paradigm and Chase AI's *"Karpathy's Obsidian RAG + Claude Code = CHEAT CODE"* workflow.

---

## 🌟 Why This Architecture is a "Cheat Code"

Traditional **Retrieval-Augmented Generation (RAG)**:
- Chunks text arbitrarily and embeds into vector databases.
- Is **stateless**: every query re-discovers knowledge from scratch without compounding.
- Often struggles with multi-document cross-synthesis and contradiction resolution.

**Karpathy's LLM Wiki**:
- Moves from **stateless runtime retrieval** to **ahead-of-time knowledge compilation**.
- When new sources arrive in `raw/`, the LLM agent compiles them into structured, interlinked notes in `wiki/`.
- Cross-references, synthesis, and contradiction resolution are baked directly into the knowledge graph via human-readable Markdown and `[[WikiLinks]]`.
- **Obsidian is the IDE; the LLM is the programmer; the wiki is the codebase.**

---

## 📂 Vault Architecture

```
DeltaSync/ (Your Obsidian Vault & Project Root)
│
├── raw/                         # Layer 1: Immutable source materials
│   ├── assets/                  # Local images and media attachments
│   └── karpathy-llm-wiki.md     # Seed reference document
│
├── wiki/                        # Layer 2: Persistent synthesized knowledge
│   ├── LLM Wiki Architecture.md
│   ├── Compiling vs Retrieving.md
│   ├── Obsidian as Knowledge IDE.md
│   └── Persistent Agent Memory.md
│
├── outputs/                     # Layer 3: Deliverables (Marp slides, briefs, reports)
│
├── index.md                     # Master catalog of wiki entries (updated on every ingest)
├── log.md                       # Chronological append-only operations log
├── CLAUDE.md                    # Claude Code operating manual
├── AGENTS.md                    # Universal AI agent instructions
│
├── scripts/
│   └── wiki_tool.py             # CLI utility for linting, reindexing, stats, and search
│
└── .claude/skills/
    └── llm-wiki/SKILL.md        # Native Claude Code custom skill
```

---

## 🚀 How to Use the Setup

### 1. Daily Ingestion Flow
1. **Clip or Save Articles**: Drop articles, research papers, YouTube transcripts, or notes into `raw/`.
   - *Tip*: Use the official [Obsidian Web Clipper](https://obsidian.md/clipper) extension with [`docs/OBSIDIAN_WEB_CLIPPER_TEMPLATE.json`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/docs/OBSIDIAN_WEB_CLIPPER_TEMPLATE.json).
2. **Run Claude Code or Antigravity**:
   Open a terminal in this vault and run:
   ```bash
   claude
   ```
   Or interact directly right here in Antigravity chat.
3. **Ask to Ingest**:
   ```
   /ingest raw/your-article.md
   ```
   The agent will:
   - Read the raw file without altering it.
   - Cross-reference with `index.md`.
   - Update existing `wiki/` notes and create new concept/entity notes.
   - Weave `[[bidirectional links]]`.
   - Re-index `index.md` and append a timestamped entry to `log.md`.

---

### 2. Querying Your Second Brain
Ask high-level synthesis questions:
```
/query What are the key architectural differences between vector RAG and compiled wikis?
```
The agent checks `index.md`, reads the relevant notes in `wiki/`, and delivers a synthesized answer with file citations. If the inquiry produces a novel comparison or insight, ask the agent:
> *"File this analysis as a new synthesis note in wiki/."*

---

### 3. Vault Health & Auditing (Linting)
Ensure your knowledge graph remains cohesive and free of dead ends:
```bash
python scripts/wiki_tool.py lint
```
Or in chat:
```
/lint
```
The tool automatically checks for:
- ❌ Broken `[[links]]` (targets that do not exist).
- ⚠️ Orphan notes (notes that have no incoming links from other pages).
- ⚠️ Incomplete YAML frontmatter (`type`, `tags`).
- 🔄 Generates a health report with action items.

---

### 4. Viewing Vault Metrics
Inspect knowledge density and connection growth:
```bash
python scripts/wiki_tool.py stats
```
Example output:
```
=======================================================
 🧠  KARPATHY LLM WIKI / OBSIDIAN VAULT STATISTICS
=======================================================
  • Raw Ingestion Documents:  1
  • Compiled Wiki Pages:      4
  • Total Cross-References:   17
  • Average Link Density:     4.25 links/page
  • Orphan Pages (0 inbound): 0
  • Broken Link References:   0
=======================================================
```

---

## 🎨 Recommended Obsidian Settings

1. **Enable WikiLinks**:
   - Go to **Settings → Files and links → Use [[Wikilinks]]** (Ensure this is turned **ON**).
2. **Set Attachment Folder**:
   - Go to **Settings → Files and links → Default location for new attachments** → Choose *"In the folder specified below"*.
   - Set folder to: `raw/assets`.
3. **Recommended Community Plugins**:
   - **Dataview**: For querying frontmatter metadata.
   - **Marp Slides**: For presenting markdown notes in `outputs/` directly as slides.
   - **Omnisearch**: High-speed fuzzy search across the vault.
