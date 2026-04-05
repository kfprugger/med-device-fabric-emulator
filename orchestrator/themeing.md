# Microsoft Fabric Color Theming & UI Design Guide
**(Internal – for GitHub Copilot / Engineering & Design Enablement)**

This document provides an authoritative overview of **Microsoft Fabric color theming and UI design**, aligned with Microsoft‑published guidance.  
It is intended for **internal consumption** by engineers, architects, and designers building **reports, extensions, or customer‑facing assets** that touch Microsoft Fabric.

---

## 1. Governing Design Systems in Microsoft Fabric

Microsoft Fabric UI is governed by **three stacked systems**. Understanding the separation is critical to knowing what is customizable—and what is not.

---

### 1.1 Fluent 2 Design System (Global Foundation)

Microsoft Fabric is built on **Fluent 2**, the Microsoft‑wide design system used across Microsoft 365 (Teams, Power BI, Office).

Fluent 2 defines:
- Color palettes and semantic roles
- Light / dark mode behavior
- Typography, spacing, corner radius, elevation
- Accessibility and contrast rules

**Authoritative references**
- Fluent 2 Color System  
- Fluent UI documentation

> Fluent tokens and semantics must be used instead of raw hex values when extending or embedding Fabric‑aligned experiences.

---

### 1.2 Fabric UX System (Fabric‑Specific Layer)

The **Fabric UX System** is a Fabric‑specific layer on top of Fluent 2. It governs:

- Workspace layouts
- Navigation patterns
- Cards, ribbons, command bars
- Side navigation behavior
- Extension and customization boundaries

This system defines **how Fabric experiences should look and behave**, ensuring consistency across tenants and workloads.

> This is the primary source of truth for anyone extending or designing within the Fabric UI surface.

---

### 1.3 Power BI / Fabric Item Layer

Anything tied to **reports, datasets, or semantic models** follows **Power BI theming rules**, including:

- Base themes (Classic 2026, Fluent 2 preview)
- JSON‑based report themes
- Visual defaults and color ramps

These settings apply **inside reports only** and do **not** affect Fabric workspace chrome or editors.

---

## 2. What Can and Cannot Be Themed

### 2.1 Not Customizable (Locked)

The following are **not customizable** and must not be overridden:

- Fabric workspace chrome
- Navigation colors
- Editors (Notebooks, Lakehouse explorer, SQL editor, pipelines)
- Core Fabric UI surfaces

These are locked intentionally to ensure **cross‑tenant consistency and accessibility**.

---

### 2.2 Supported Customization Surfaces

#### A. Power BI Report & Semantic Model Themes
- Fully supported via JSON themes
- Can leverage Fluent‑aligned color ramps
- Applies only within the report canvas

Use this for customer branding **inside reports**, dashboards, and visuals.

---

#### B. Tenant‑Level Branding (Admins Only)
Fabric administrators may configure:
- Header theme color
- Organization logo
- Home page cover image

**Important:**  
This impacts the **Power BI service UI only**, not Fabric editors or workspace UI.

---

#### C. Extensions / Custom Experiences
For:
- Custom Fabric extensions
- Embedded UI
- Partner workloads

**Requirements**
- Use Fluent tokens (semantic, neutral, brand)
- Do not hard‑code hex colors
- Honor light/dark mode automatically

---

## 3. Fabric Color Model (High‑Level)

Fabric follows the Fluent **three‑palette model**.

---

### 3.1 Neutral Palette
Used for:
- Surfaces
- Containers
- Text
- Background hierarchy

Examples:
- Canvas backgrounds
- List panes
- Editor surfaces

Purpose: **readability, hierarchy, focus**

---

### 3.2 Shared / Semantic Colors
Used to communicate meaning:
- Success
- Warning
- Error
- Info
- Selection and focus states

Rules:
- Semantic colors must convey meaning
- Must not be reused for decoration or branding

---

### 3.3 Brand Colors
Used sparingly to anchor product identity:
- Power BI blue
- Fabric gradients
- Accent highlights

Rules:
- Do not use on large surfaces
- Avoid overwhelming neutral hierarchy

---

## 4. Official Design Assets (Use These)

### 4.1 Microsoft Fabric UI Kit (Figma)

The **official** design kit for Fabric experiences. Includes:
- Fluent 2 tokens
- Fabric‑approved components
- Spacing, radius, color variables
- Updated navigation and card patterns

This should be the **default starting point** for any Fabric UI design work.

---

### 4.2 Fluent UI Theme Designer

Use this tool to:
- Experiment with token‑based themes
- Validate accessibility contrast
- Prototype safely before implementation

---

### 4.3 Architecture & Diagram Assets

For **documentation, decks, and architecture diagrams** (not product UI):
- Fabric color‑coded Visio stencils
- Consistent with branding expectations
- Safe for customer‑facing materials

---

## 5. Practical Engineering & Design Guidance

### ✅ Do
- Use Fluent semantic tokens (e.g., neutralForeground, accentPrimary)
- Follow Fluent 2 spacing and radius scales
- Limit branding to report themes and diagrams
- Assume light/dark mode support is mandatory

### ❌ Don’t
- Hard‑code hex colors in extensions
- Attempt per‑workspace UI theming
- Override navigation, chrome, or editor colors
- Repurpose semantic colors for decoration

---

## 6. Recommended Entry Points (TL;DR)

If you only reference **three things**, use these:

1. **Fabric UX System** – Rules of the road for Fabric UI  
2. **Fluent 2 Color System** – How color semantics work  
3. **Microsoft Fabric UI Kit (Figma)** – What to design against  

---

## 7. Common Internal Scenarios

- **Customer wants branding in Fabric**
  - ✅ Use Power BI report theming
  - ✅ Use diagrams/decks with Fabric stencils
  - ❌ Do not promise workspace UI theming

- **Building a Fabric extension**
  - ✅ Use Fluent tokens
  - ✅ Follow Fabric UX System patterns
  - ❌ Avoid raw colors and custom chrome

---

**Ownership:** Microsoft Fabric UX / Fluent Design  
**Intended Audience:** Internal engineering, design, CSA, and partner teams