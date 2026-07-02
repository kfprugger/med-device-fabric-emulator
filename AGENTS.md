# AGENTS.md

This project ships Rayfin agent context.
Load `.agents/skills/rayfin/SKILL.md` and the `rayfin` MCP server in `.mcp.json` before writing Rayfin code.

Rayfin docs are version-locked to the packages installed in this project.
Prefer the MCP tools `search_docs`, `get_doc`, `list_docs`, and `discover_packages` for examples, API details, and troubleshooting.
If MCP is unavailable, run `rayfin docs ...` from the project root so the CLI reads this project's `node_modules`.
If `rayfin` is not on `PATH`, use `npx -y @microsoft/rayfin-cli docs ...` from the project root.

Use `discover_packages` or `rayfin docs discover <topic>` when installed docs do not cover the task.

## Project deployment memory

- `FabricDicomCohortingToolkit` is a required downstream companion repo for imaging deployment paths that execute the Phase 3 toolkit block. `Deploy-All.ps1` and `Preflight-Check.ps1` should resolve the default sibling path (`../FabricDicomCohortingToolkit`) and auto-clone `https://github.com/kfprugger/FabricDicomCohortingToolkit.git` when that block requires it and the repo is missing.
- Keep the preflight requirement aligned with actual deploy control flow. If a mode executes the imaging block, preflight must also bootstrap/check the downstream repo; if a mode skips imaging, it must not require the repo.
