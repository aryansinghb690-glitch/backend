import "./globals.css";

import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "SagePilot Workflow Builder",
  description: "Workflow automation editor and runtime",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className="bg-white text-slate-900 dark:bg-slate-950 dark:text-slate-100">{children}</body>
    </html>
  );
}
