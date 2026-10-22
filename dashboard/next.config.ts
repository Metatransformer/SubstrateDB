import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "export",
  basePath: "/dashboard",
  assetPrefix: "/dashboard",
  trailingSlash: true,
  reactStrictMode: true,
};

export default nextConfig;
