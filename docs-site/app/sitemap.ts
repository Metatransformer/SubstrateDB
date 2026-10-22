import type { MetadataRoute } from "next";
import { DOCS, SITE_ORIGIN } from "../lib/docs";

export const dynamic = "force-static";

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    {
      url: SITE_ORIGIN,
      changeFrequency: "weekly",
      priority: 1,
    },
    ...DOCS.map((doc) => ({
      url: `${SITE_ORIGIN}/${doc.slug}/`,
      changeFrequency: "weekly" as const,
      priority: doc.slug === "quickstart" ? 0.9 : 0.7,
    })),
  ];
}
