# Video Generation with Remotion

This directory contains a [Remotion](https://remotion.dev/) project that generates the animated overview video for the Medical Device FHIR Integration Platform.

## What It Produces

A 1920x1080 MP4 video (~3 minutes) with 8 animated slides:

1. **Title** — Platform name, tech badges, gradient background
2. **Capabilities** — 6 bullet points with staggered reveal
3. **Architecture** — Animated box diagram (Azure RG + Fabric workspace)
4. **Phase 1** — Infrastructure & Ingestion steps
5. **Phase 2** — HDS Enrichment & Data Agents steps
6. **Phase 3** — Imaging & Cohorting steps
7. **Data Agents** — 3-column cards (Patient 360, Clinical Triage, Cohorting)
8. **Closing** — Deploy command + stats grid

## Prerequisites

- Node.js 18+
- npm

## Quick Start

```bash
# Install dependencies
cd video
npm install

# Preview in Remotion Studio (live editing)
npx remotion studio src/index.ts

# Render to MP4
npx remotion render src/index.ts ReadmeOverview out/readme-overview.mp4
```

## How It Was Built

This video was generated entirely by an AI coding agent (GitHub Copilot) using the Remotion MCP server for documentation lookups. The workflow:

1. **Agent reads the README** — parses the project description, phases, architecture, and agent details
2. **Agent queries Remotion docs** — uses the `remotion-documentation` MCP tool to look up APIs (`Composition`, `useCurrentFrame`, `interpolate`, `spring`, `TransitionSeries`, etc.)
3. **Agent generates slide components** — creates React/TypeScript components for each slide with animations
4. **Agent renders the video** — runs `npx remotion render` to produce the MP4

### Key Remotion Concepts Used

| Concept | Usage |
|---------|-------|
| `TransitionSeries` + `fade()` | Smooth transitions between slides |
| `useCurrentFrame()` + `interpolate()` | Frame-based opacity and position animations |
| `spring()` | Physics-based entrance animations for elements |
| `AbsoluteFill` | Full-frame layering for backgrounds and content |
| `Sequence` (via `TransitionSeries.Sequence`) | Timed slide durations |

### Project Structure

```
video/
├── src/
│   ├── index.ts              # Remotion entry point
│   ├── Root.tsx               # Composition definition (id, fps, dimensions, duration)
│   ├── ReadmeVideo.tsx        # Main composition — sequences all slides
│   └── slides/
│       ├── TitleSlide.tsx     # Animated title with gradient + tech badges
│       ├── BulletSlide.tsx    # Staggered bullet point reveals
│       ├── ArchitectureSlide.tsx # Animated box diagram
│       ├── PhaseSlide.tsx     # Reusable phase detail slide
│       ├── DataFlowSlide.tsx  # Pipeline step visualization
│       ├── AgentsSlide.tsx    # 3-column agent comparison cards
│       └── ClosingSlide.tsx   # Deploy command + stats
├── out/
│   └── readme-overview.mp4   # Rendered output
├── package.json
└── tsconfig.json
```

## Customization

### Change slide content

Edit the props passed to each slide in `src/ReadmeVideo.tsx`. For example, to update Phase 1 items:

```tsx
<PhaseSlide
    phase={1}
    title="Infrastructure & Ingestion"
    color="#ff8c00"
    duration="~25 min"
    items={[
        'Your custom step 1',
        'Your custom step 2',
    ]}
/>
```

### Change timing

- **Slide duration**: Adjust `durationInFrames` on each `TransitionSeries.Sequence` (30 frames = 1 second)
- **Transition speed**: Change the `TRANSITION` constant (default: 20 frames)
- **Total duration**: Update `durationInFrames` in `Root.tsx` to match the sum

### Change resolution/FPS

Edit `Root.tsx`:

```tsx
<Composition
    width={1920}      // 4K: 3840
    height={1080}     // 4K: 2160
    fps={30}          // 60 for smoother animations
    durationInFrames={5320}
/>
```

## Tips for AI-Generated Videos

When asking an AI agent to generate a Remotion video:

1. **Provide the content** — paste or attach the README/doc you want visualized
2. **Specify the style** — "dark theme", "animated boxes", "staggered bullet reveals"
3. **Request full-frame usage** — explicitly ask for content to "fill the entire 1920x1080 frame"
4. **Set reading time** — ask for "15 seconds of dwell time per slide" so viewers can read
5. **Iterate on font sizes** — rendered video fonts often look smaller than in the editor; ask to increase
6. **Use the Remotion MCP** — the agent can query Remotion docs in real-time for correct API usage
