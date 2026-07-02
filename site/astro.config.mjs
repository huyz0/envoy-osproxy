import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import remarkMermaid from "./remark-mermaid.mjs";

const repo = "https://github.com/huyz0/envoy-osproxy";

// Loads mermaid from a CDN and renders every <pre class="mermaid"> on each page,
// including after Starlight's client-side navigation. Two readability affordances:
//   1. `useMaxWidth: false` keeps the natural font size (the `.mermaid` wrapper
//      scrolls horizontally instead of shrinking the whole diagram).
//   2. Click-to-zoom lightbox — a rendered diagram opens full-viewport on click and
//      dismisses on click/Escape, so big flowcharts stay legible. Dependency-free.
const mermaidBoot = `
import mermaid from "https://esm.sh/mermaid@11";
function armZoom() {
  document.querySelectorAll("pre.mermaid:not([data-zoom])").forEach((el) => {
    el.setAttribute("data-zoom", "true");
    el.style.cursor = "zoom-in";
    el.addEventListener("click", () => {
      const svg = el.querySelector("svg");
      if (!svg) return;
      const overlay = document.createElement("div");
      overlay.className = "mermaid-lightbox";
      overlay.innerHTML = el.innerHTML;
      const close = () => overlay.remove();
      overlay.addEventListener("click", close);
      const onKey = (e) => { if (e.key === "Escape") { close(); document.removeEventListener("keydown", onKey); } };
      document.addEventListener("keydown", onKey);
      document.body.appendChild(overlay);
    });
  });
}
function runMermaid() {
  const dark = document.documentElement.dataset.theme === "dark";
  mermaid.initialize({ startOnLoad: false, theme: dark ? "dark" : "default", securityLevel: "loose", fontFamily: "inherit", flowchart: { useMaxWidth: false }, sequence: { useMaxWidth: false } });
  mermaid.run({ querySelector: "pre.mermaid:not([data-rendered])" }).then(armZoom);
  document.querySelectorAll("pre.mermaid").forEach((el) => el.setAttribute("data-rendered", "true"));
}
document.addEventListener("astro:page-load", runMermaid);
if (document.readyState !== "loading") runMermaid();
`;

const mermaidCss = `
pre.mermaid { overflow-x: auto; text-align: center; }
pre.mermaid svg { max-width: none; height: auto; }
.mermaid-lightbox {
  position: fixed; inset: 0; z-index: 1000; cursor: zoom-out;
  display: flex; align-items: center; justify-content: center;
  padding: 2rem; background: rgba(0, 0, 0, 0.9);
  backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px);
}
.mermaid-lightbox svg { max-width: 96vw; max-height: 92vh; width: auto; height: auto; }
`;

export default defineConfig({
  site: "https://huyz0.github.io",
  base: "/envoy-osproxy",
  markdown: { remarkPlugins: [remarkMermaid] },
  integrations: [
    starlight({
      title: "envoy-osproxy",
      description:
        "Multi-tenant OpenSearch proxy capabilities delivered as an extension of a stock Envoy.",
      social: [{ icon: "github", label: "GitHub", href: repo }],
      head: [
        { tag: "script", attrs: { type: "module" }, content: mermaidBoot },
        { tag: "style", content: mermaidCss },
      ],
      sidebar: [
        { label: "Start here", items: [{ label: "Introduction", link: "/" }] },
        {
          label: "Guide",
          items: [
            { label: "Architecture", link: "/01-architecture/" },
            { label: "Implementing a tenancy", link: "/02-tenancy/" },
            { label: "Building the ext_proc backend", link: "/03-build-extproc/" },
            { label: "Building the dynamic module", link: "/04-build-module/" },
            { label: "ext_proc vs. dynamic module", link: "/05-backends/" },
            { label: "Benchmarks", link: "/06-benchmarks/" },
          ],
        },
      ],
    }),
  ],
});
