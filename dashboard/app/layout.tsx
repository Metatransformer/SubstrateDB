import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Substrate DB Console",
  description: "Local administration dashboard for SubstrateDB",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
