# Documentation Platform Decision

## Context

Backlog item: documentation UX refresh required a better landing page, faster navigation/search ergonomics, and an explicit decision whether to keep the current docs stack or migrate.

## Requirements

- Open-source tooling only.
- Low operational overhead for contributors.
- Static hosting support (GitHub Pages).
- Strong search/navigation UX.
- Easy docs review in pull requests.
- No runtime coupling to Hookaido binary behavior.

## Options Evaluated

### Option A: Keep MkDocs Material (current)

Pros:
- Mature OSS stack with stable static output.
- Existing project setup already works on GitHub Pages.
- Built-in search UX and navigation features.
- Minimal migration risk and no content rewrite needed.

Cons:
- Advanced UX patterns (for example command-palette behavior) require small custom JS/CSS.

### Option B: Docusaurus

Pros:
- Strong React ecosystem and plugin model.
- Good docs versioning capabilities.

Cons:
- Requires a Node.js toolchain and JS framework conventions.
- Migration would require template/content restructuring.

### Option C: Astro Starlight

Pros:
- Modern docs UX and performance profile.
- Flexible theming and component model.

Cons:
- Node.js toolchain and migration cost.
- Higher maintenance overhead versus current setup.

## Decision

Keep MkDocs Material for the current roadmap window.

## Rationale

- Meets all project requirements with the least delivery risk.
- Supports the refreshed UX directly (hero landing, grouped nav, improved search behavior).
- Avoids migration effort that would not improve Hookaido runtime capabilities.
- Keeps contributor workflow simple and aligned with existing docs CI/deploy.

## Re-evaluation Triggers

Re-open migration discussion if one or more apply:

- Docs need product/version split with independent IA that becomes hard to maintain in current setup.
- Required UX behavior cannot be implemented without heavy custom JS/CSS.
- Build/deploy constraints change away from static GitHub Pages assumptions.

## Next Review Point

Revisit this decision after the next major feature wave (post optional gRPC worker API scope), or sooner if any trigger above is hit.
